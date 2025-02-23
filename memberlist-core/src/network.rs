use core::sync::atomic::Ordering;

use crate::transport::Connection;

use super::{base::Memberlist, delegate::Delegate, error::Error, proto::*, transport::Transport};

use agnostic_lite::RuntimeLite;
use bytes::{Buf, Bytes};
use futures::{
  future::FutureExt,
  stream::{FuturesUnordered, Stream},
};
use nodecraft::resolver::AddressResolver;

mod packet;
mod stream;

/// Maximum size for node meta data
pub const META_MAX_SIZE: usize = 512;

/// Maximum number of concurrent push/pull requests
const MAX_PUSH_PULL_REQUESTS: u32 = 128;

impl<D, T> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) async fn send_ping_and_wait_for_ack(
    &self,
    target: &<T::Resolver as AddressResolver>::ResolvedAddress,
    ping: Ping<T::Id, T::ResolvedAddress>,
    deadline: <T::Runtime as RuntimeLite>::Instant,
  ) -> Result<bool, Error<T, D>> {
    let conn = match self.inner.transport.open(target, deadline).await {
      Ok(conn) => conn,
      Err(_) => {
        // If the node is actually dead we expect this to fail, so we
        // shouldn't spam the logs with it. After this point, errors
        // with the connection are real, unexpected errors and should
        // get propagated up.
        return Ok(false);
      }
    };

    let ping_sequence_number = ping.sequence_number();
    let (mut reader, mut writer) = conn.split();
    let res = <T::Runtime as RuntimeLite>::timeout_at(deadline, async {
      self
        .send_message(&mut writer, [Message::Ping(ping)])
        .await?;
      self.read_message(target, &mut reader).await
    })
    .await;

    let mut msg = match res {
      Ok(Ok(msg)) => msg,
      Ok(Err(e)) => return Err(e),
      Err(e) => return Err(Error::transport(std::io::Error::from(e).into())),
    };

    if msg.is_empty() {
      return Err(Error::custom("receive empty message".into()));
    }
    let mt = match MessageType::try_from(msg[0]) {
      Ok(mt) => mt,
      Err(val) => return Err(Error::UnknownMessageType(val)),
    };
    msg.advance(1);

    if let MessageType::Ack = mt {
      let seqn = match Ack::decode_sequence_number(&msg) {
        Ok(seqn) => seqn.1,
        Err(e) => return Err(e.into()),
      };

      if seqn != ping_sequence_number {
        return Err(Error::sequence_number_mismatch(ping_sequence_number, seqn));
      }

      Ok(true)
    } else {
      Err(Error::unexpected_message(MessageType::Ack, mt))
    }
  }

  /// Returns an messages processor to encode/compress/encrypt messages
  pub(crate) fn unreliable_encoder<'a, M>(
    &'a self,
    packets: M,
  ) -> ProtoEncoder<T::Id, T::ResolvedAddress, M>
  where
    M: AsRef<[Message<T::Id, T::ResolvedAddress>]> + Send + Sync + 'a,
  {
    #[allow(unused_mut)]
    let mut encoder = ProtoEncoder::new(self.inner.transport.max_packet_size())
      .with_messages(packets)
      .with_label(self.inner.opts.label().clone())
      .with_overhead(self.inner.transport.header_overhead());

    #[cfg(checksum)]
    if !self.inner.transport.packet_reliable() {
      encoder.maybe_checksum(self.inner.opts.checksum_algo());
    }

    #[cfg(feature = "encryption")]
    if !self.inner.transport.packet_secure() && self.encryption_enabled() {
      encoder.set_encryption(
        self.inner.opts.encryption_algo().unwrap(),
        self.inner.keyring.as_ref().unwrap().primary_key(),
      );
    }

    #[cfg(compression)]
    encoder.maybe_compression(self.inner.opts.compress_algo());

    encoder
  }

  /// Returns an messages processor to encode/compress/encrypt messages
  pub(crate) fn reliable_encoder<'a, M>(
    &'a self,
    packets: M,
  ) -> ProtoEncoder<T::Id, T::ResolvedAddress, M>
  where
    M: AsRef<[Message<T::Id, T::ResolvedAddress>]> + Send + Sync + 'a,
  {
    #[allow(unused_mut)]
    let mut encoder = ProtoEncoder::new(self.inner.transport.max_packet_size())
      .with_messages(packets)
      .with_label(self.inner.opts.label().clone())
      .with_overhead(self.inner.transport.header_overhead());

    #[cfg(feature = "encryption")]
    if !self.inner.transport.stream_secure() && self.encryption_enabled() {
      encoder.set_encryption(
        self.inner.opts.encryption_algo().unwrap(),
        self.inner.keyring.as_ref().unwrap().primary_key(),
      );
    }

    #[cfg(compression)]
    encoder.maybe_compression(self.inner.opts.compress_algo());

    encoder
  }

  #[auto_enums::auto_enum(futures03::Stream)]
  pub(crate) async fn transport_send_packets<'a, M>(
    &'a self,
    addr: &'a T::ResolvedAddress,
    msgs: M,
  ) -> impl Stream<Item = Result<(), Error<T, D>>> + Send + 'a
  where
    M: AsRef<[Message<T::Id, T::ResolvedAddress>]> + Send + Sync + 'static,
  {
    let encoder = self.unreliable_encoder(msgs);
    match encoder.hint() {
      Err(e) => futures::stream::once(async { Err(e.into()) }),
      Ok(hint) => {
        #[cfg(not(offload))]
        {
          let _ = hint;
          FuturesUnordered::from_iter(encoder.encode().map(|res| match res {
            Ok(payload) => futures::future::Either::Left(self.raw_send_packet(addr, payload)),
            Err(e) => futures::future::Either::Right(Self::to_async_err(e.into())),
          }))
        }

        #[cfg(offload)]
        {
          match hint.should_offload(self.inner.opts.offload_size) {
            false => FuturesUnordered::from_iter(encoder.encode().map(|res| match res {
              Ok(payload) => futures::future::Either::Left(self.raw_send_packet(addr, payload)),
              Err(e) => futures::future::Either::Right(Self::to_async_err(e.into())),
            })),
            true => {
              #[cfg(not(feature = "rayon"))]
              {
                let payloads = encoder.blocking_encode::<T::Runtime>().await;
                FuturesUnordered::from_iter(payloads.into_iter().map(|res| match res {
                  Ok(payload) => futures::future::Either::Left(self.raw_send_packet(addr, payload)),
                  Err(e) => futures::future::Either::Right(Self::to_async_err(e.into())),
                }))
              }

              #[cfg(feature = "rayon")]
              {
                use rayon::iter::ParallelIterator;

                let payloads = encoder
                  .rayon_encode()
                  .filter_map(|res| match res {
                    Ok(payload) => Some(payload),
                    Err(e) => {
                      tracing::error!(err = %e, "memberlist.pakcet: failed to process packet");
                      None
                    }
                  })
                  .collect::<Vec<_>>();

                FuturesUnordered::from_iter(payloads.into_iter().map(|payload| {
                  futures::future::Either::Left(self.raw_send_packet(addr, payload))
                }))
              }
            }
          }
        }
      }
    }
  }

  pub(crate) async fn send_message<'a, M>(
    &'a self,
    conn: &'a mut <T::Connection as Connection>::Writer,
    msgs: M,
  ) -> Result<(), Error<T, D>>
  where
    M: AsRef<[Message<T::Id, T::ResolvedAddress>]> + Send + Sync + 'static,
  {
    let encoder = self.reliable_encoder(msgs);

    match encoder.hint() {
      Err(e) => Err(e.into()),
      Ok(hint) => {
        #[cfg(not(offload))]
        {
          let _ = hint;
          let mut errs = OneOrMore::new();
          for res in encoder.encode() {
            match res {
              Ok(payload) => match self.raw_send_message(conn, payload).await {
                Ok(()) => {}
                Err(e) => errs.push(e),
              },
              Err(e) => errs.push(e.into()),
            }
          }

          Error::try_from_one_or_more(errs)
        }

        #[cfg(offload)]
        {
          match hint.should_offload(self.inner.opts.offload_size) {
            false => {
              let mut errs = OneOrMore::new();
              for res in encoder.encode() {
                match res {
                  Ok(payload) => match self.raw_send_message(conn, payload).await {
                    Ok(()) => {}
                    Err(e) => errs.push(e),
                  },
                  Err(e) => errs.push(e.into()),
                }
              }

              Error::try_from_one_or_more(errs)
            }
            true => {
              #[cfg(not(feature = "rayon"))]
              {
                let mut errs = OneOrMore::new();
                let payloads = encoder
                  .blocking_encode::<T::Runtime>()
                  .await
                  .filter_map(|res| match res {
                    Ok(payload) => Some(payload),
                    Err(e) => {
                      tracing::error!(err = %e, "memberlist.pakcet: failed to process packet");
                      None
                    }
                  });

                for payload in payloads {
                  match self.raw_send_message(conn, payload).await {
                    Ok(()) => {}
                    Err(e) => errs.push(e),
                  }
                }

                Error::try_from_one_or_more(errs)
              }

              #[cfg(feature = "rayon")]
              {
                use rayon::iter::ParallelIterator;

                let payloads = encoder
                  .rayon_encode()
                  .filter_map(|res| match res {
                    Ok(payload) => Some(payload),
                    Err(e) => {
                      tracing::error!(err = %e, "memberlist.pakcet: failed to process packet");
                      None
                    }
                  })
                  .collect::<Vec<_>>();

                let mut errs = OneOrMore::new();
                for payload in payloads {
                  match self.raw_send_message(conn, payload).await {
                    Ok(()) => {}
                    Err(e) => errs.push(e),
                  }
                }

                Error::try_from_one_or_more(errs)
              }
            }
          }
        }
      }
    }
  }

  pub(crate) async fn read_message(
    &self,
    from: &<T::Resolver as AddressResolver>::ResolvedAddress,
    reader: &mut <T::Connection as Connection>::Reader,
  ) -> Result<Bytes, Error<T, D>> {
    self
      .inner
      .transport
      .read(from, reader)
      .await
      .map_err(Error::transport)?;

    let mut decoder = ProtoDecoder::new();

    #[cfg(offload)]
    decoder.with_offload_size(self.inner.opts.offload_size);

    #[cfg(feature = "encryption")]
    if self.encryption_enabled() {
      decoder
        .with_encryption(triomphe::Arc::from_iter(
          self.inner.keyring.as_ref().unwrap().keys(),
        ))
        .with_verify_incoming(self.inner.opts.gossip_verify_incoming);
    }

    if !self.inner.opts.skip_inbound_label_check {
      decoder.with_label(self.inner.opts.label().clone());
    }

    decoder
      .decode_from_reader::<_, T::Runtime>(reader)
      .await
      .map_err(|e| Error::transport(e.into()))
  }

  async fn raw_send_packet<'a>(
    &'a self,
    addr: &'a T::ResolvedAddress,
    payload: Payload,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .send_to(addr, payload)
      .await
      .map(|(_sent, _)| {
        #[cfg(feature = "metrics")]
        {
          metrics::counter!(
            "memberlist.packet.sent",
            self.inner.opts.metric_labels.iter()
          )
          .increment(_sent as u64);
        }
      })
      .map_err(Error::transport)
  }

  async fn raw_send_message<'a>(
    &'a self,
    conn: &'a mut <T::Connection as Connection>::Writer,
    payload: Payload,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .write(conn, payload)
      .await
      .map(|_sent| {
        #[cfg(feature = "metrics")]
        {
          metrics::counter!(
            "memberlist.stream.sent",
            self.inner.opts.metric_labels.iter()
          )
          .increment(_sent as u64);
        }
      })
      .map_err(Error::transport)
  }

  async fn to_async_err(e: Error<T, D>) -> Result<(), Error<T, D>>
  where
    T: Transport,
    D: Delegate,
  {
    Err(e)
  }
}
