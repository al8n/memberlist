use core::sync::atomic::Ordering;

use super::{base::Memberlist, delegate::Delegate, error::Error, transport::Transport, types::*};

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
    let mut conn: T::Stream = match self
      .inner
      .transport
      .dial_with_deadline(target, deadline)
      .await
    {
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
    let msgs = [Message::Ping(ping)];

    let res = <T::Runtime as RuntimeLite>::timeout_at(deadline, async {
      self.send_message(&mut conn, &msgs).await?;
      self
        .read_message(target, &mut conn)
        .await
        .map(|(_, msg)| msg)
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
      // TODO: handle error correctly
      Err(_e) => return Err(Error::unexpected_message("Ack", "unknown")),
    };
    msg.advance(1);

    if let MessageType::Ack = mt {
      let seqn = match Ack::decode_sequence_number(&msg) {
        Ok(seqn) => seqn.1,
        // TODO: handle error correctly
        Err(e) => {
          return Err(Error::custom(
            format!("failed to decode ack sequence number: {}", e).into(),
          ))
        }
      };

      if seqn != ping_sequence_number {
        return Err(Error::sequence_number_mismatch(ping_sequence_number, seqn));
      }

      if let Err(e) = self.inner.transport.close(target, conn).await {
        tracing::warn!(local_addr = %self.inner.id, peer_addr = %target, err = %e, "memberlist.transport: failed to close stream");
      }

      Ok(true)
    } else {
      Err(Error::UnexpectedMessage {
        expected: "Ack",
        got: mt.kind(),
      })
    }
  }

  /// Returns an messages processor to encode/compress/encrypt messages
  pub(crate) fn encoder<'a>(
    &'a self,
    packets: &'a [Message<T::Id, T::ResolvedAddress>],
  ) -> ProtoEncoder<'a, T::Id, T::ResolvedAddress> {
    let mut encoder =
      ProtoEncoder::<T::Id, T::ResolvedAddress>::new(self.inner.transport.max_packet_size());
    encoder
      .with_messages(packets)
      .with_label(self.inner.opts.label());

    #[cfg(any(
      feature = "crc32",
      feature = "xxhash64",
      feature = "xxhash32",
      feature = "xxhash3",
      feature = "murmur3",
    ))]
    if !self.inner.transport.packet_reliable() {
      encoder.maybe_checksum(self.inner.opts.checksum_algo());
    }

    #[cfg(feature = "encryption")]
    if !self.inner.transport.packet_secure() && self.encryption_enabled() {
      encoder.with_encryption(
        self.inner.opts.encryption_algo().unwrap(),
        self.inner.keyring.as_ref().unwrap().primary_key(),
      );
    }

    #[cfg(any(
      feature = "zstd",
      feature = "lz4",
      feature = "brotli",
      feature = "deflate",
      feature = "gzip",
      feature = "snappy",
      feature = "lzw",
      feature = "zlib",
    ))]
    encoder.maybe_compression(self.inner.opts.compress_algo());

    encoder
  }

  #[auto_enums::auto_enum(futures03::Stream)]
  pub(crate) fn transport_send_packets<'a>(
    &'a self,
    addr: &'a T::ResolvedAddress,
    msgs: &[Message<T::Id, T::ResolvedAddress>],
  ) -> impl Stream<Item = Result<(), Error<T, D>>> + Send + 'a {
    let encoder = self.encoder(msgs);
    match encoder.hint() {
      Err(e) => futures::stream::once(async { Err(e.into()) }),
      Ok(hint) => {
        cfg_if::cfg_if! {
          if #[cfg(any(
            feature = "zstd",
            feature = "lz4",
            feature = "brotli",
            feature = "snappy",
            feature = "encryption",
          ))] {
            match hint.should_offload(self.inner.opts.offload_size) {
              false => FuturesUnordered::from_iter(encoder.encode().map(|res| match res {
                Ok(payload) => futures::future::Either::Left(
                  self.raw_send_packet(addr, payload),
                ),
                Err(e) => futures::future::Either::Right(async { Err(e.into()) }),
              })),
              true => {
                use rayon::iter::ParallelIterator;

                let payloads = encoder
                  .encode_parallel()
                  .filter_map(|res| match res {
                    Ok(payload) => Some(payload),
                    Err(e) => {
                      tracing::error!(err = %e, "memberlist.pakcet: failed to process packet");
                      None
                    }
                  })
                  .collect::<Vec<_>>();

                FuturesUnordered::from_iter(payloads.into_iter().map(|payload| {
                  futures::future::Either::Left(
                    self.raw_send_packet(addr, payload),
                  )
                }))
              }
            }
          } else {
            FuturesUnordered::from_iter(encoder.encode().map(|res| match res {
              Ok(payload) => futures::future::Either::Left(
                self.raw_send_packet(addr, Ok(payload)),
              ),
              Err(e) => futures::future::Either::Right(async { Err(e.into()) }),
            }))
          }
        }
      }
    }
  }

  pub(crate) async fn send_message<'a>(
    &'a self,
    conn: &'a mut T::Stream,
    msgs: &[Message<T::Id, T::ResolvedAddress>],
  ) -> Result<(), Error<T, D>> {
    let encoder = self.encoder(msgs);

    match encoder.hint() {
      Err(e) => Err(e.into()),
      Ok(hint) => {
        cfg_if::cfg_if! {
          if #[cfg(any(
            feature = "zstd",
            feature = "lz4",
            feature = "brotli",
            feature = "snappy",
            feature = "encryption",
          ))] {
            match hint.should_offload(self.inner.opts.offload_size) {
              false => {
                let mut errs = OneOrMore::new();
                for res in encoder.encode() {
                  match res {
                    Ok(payload) => {
                      match self.raw_send_message(conn, payload).await {
                        Ok(()) => {}
                        Err(e) => errs.push(e),
                      }
                    }
                    Err(e) => errs.push(e.into()),
                  }
                }

                Error::try_from_one_or_more(errs)
              },
              true => {
                use rayon::iter::ParallelIterator;

                let payloads = encoder
                  .encode_parallel()
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
          } else {
            let mut errs = OneOrMore::new();
            for res in encoder.encode() {
              match res {
                Ok(payload) => {
                  match self.raw_send_message(conn, payload).await {
                    Ok(()) => {}
                    Err(e) => errs.push(e),
                  }
                }
                Err(e) => errs.push(e.into()),
              }
            }

            Error::try_from_one_or_more(errs)
          }
        }
      }
    }
  }

  pub(crate) async fn read_message(
    &self,
    from: &<T::Resolver as AddressResolver>::ResolvedAddress,
    conn: &mut T::Stream,
  ) -> Result<(usize, Bytes), Error<T, D>> {
    let readed = self
      .inner
      .transport
      .read(from, conn)
      .await
      .map_err(Error::transport)?;

    // Decrypt/Decompress/Decode the message

    todo!()
  }

  async fn raw_send_packet<'a>(
    &'a self,
    addr: &'a T::ResolvedAddress,
    payload: Vec<u8>,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .send_to(addr, payload.into())
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
    conn: &'a mut T::Stream,
    payload: Vec<u8>,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .write(conn, payload.into())
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
}
