use std::sync::atomic::Ordering;

use super::{
  base::Memberlist,
  delegate::Delegate,
  error::Error,
  transport::{TimeoutableStream, Transport},
  types::*,
};

use agnostic_lite::RuntimeLite;
use bytes::{Buf, Bytes};
use futures::future::FutureExt;
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
    ping: Ping<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
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
    conn.set_deadline(Some(deadline));

    let ping_sequence_number = ping.sequence_number();
    self.send_message(&mut conn, ping.into()).await?;
    let mut msg = self
      .read_message(target, &mut conn)
      .await
      .map(|(_, msg)| msg)?;

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

      if let Err(e) = self.inner.transport.cache_stream(target, conn).await {
        tracing::warn!(local_addr = %self.inner.id, peer_addr = %target, err = %e, "memberlist.transport: failed to cache stream");
      }

      Ok(true)
    } else {
      Err(Error::UnexpectedMessage {
        expected: "Ack",
        got: mt.kind(),
      })
    }
  }

  pub(crate) async fn transport_send_packets(
    &self,
    addr: &T::ResolvedAddress,
    packets: TinyVec<Message<T::Id, T::ResolvedAddress>>,
  ) -> Result<(), Error<T, D>> {
    let mut encoder =
      MessageEncoder::<T::Id, T::ResolvedAddress>::new(self.inner.transport.max_packet_size());
    encoder
      .with_messages(&packets)
      .with_label(self.inner.opts.label().as_str());

    #[cfg(any(
      feature = "crc32",
      feature = "xxhash64",
      feature = "xxhash32",
      feature = "xxhash3",
      feature = "murmur3",
    ))]
    if !self.inner.transport.packet_reliable() {
      encoder.with_checksum(self.inner.opts.checksum_algo());
    }

    #[cfg(feature = "encryption")]
    if !self.inner.transport.packet_secure() {
      encoder.with_encryption(self.inner.opts.encryption_algo());
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
    encoder.with_compression(self.inner.opts.compress_algo());

    let payload = encoder.encode()?;

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

  pub(crate) async fn send_message(
    &self,
    conn: &mut T::Stream,
    msg: Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    let mut encoder =
      MessageEncoder::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::new(usize::MAX);
    let msgs = [msg];
    encoder
      .with_messages(&msgs)
      .with_label(self.inner.opts.label().as_str());

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
    encoder.with_compression(self.inner.opts.compress_algo());

    #[cfg(feature = "encryption")]
    if !self.inner.transport.stream_secure() {
      encoder.with_encryption(self.inner.opts.encryption_algo());
    }

    let payload = encoder.encode()?;

    self
      .inner
      .transport
      .send_message(conn, payload.into())
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
      .map_err(Error::transport)?;
    Ok(())
  }

  pub(crate) async fn read_message(
    &self,
    from: &<T::Resolver as AddressResolver>::ResolvedAddress,
    conn: &mut T::Stream,
  ) -> Result<(usize, Bytes), Error<T, D>> {
    self
      .inner
      .transport
      .read_message(from, conn)
      .await
      .map_err(Error::transport)
  }
}
