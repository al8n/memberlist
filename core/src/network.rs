use std::sync::atomic::Ordering;

use super::{
  base::Memberlist,
  checksum::checksum,
  compress::compress_into_vec,
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

  pub(crate) async fn transport_send_packet(
    &self,
    addr: &T::ResolvedAddress,
    packet: Message<T::Id, T::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    let reliable = self.inner.transport.packet_reliable();
    let secure = self.inner.transport.packet_secure();
    let encoded_len = packet.encoded_len();
    let offload_size = self.inner.opts.offload_size();

    let cks_algo = self.inner.opts.checksum_algo();
    let encryption_algo = self.inner.opts.encryption_algo();
    let compress_algo = self.inner.opts.compress_algo();
    let label = self.inner.opts.label().clone();

    let need_cks = cks_algo.is_some() && !reliable;
    let need_compress = compress_algo.is_some();
    let need_encrypt = encryption_algo.is_some() && !secure;
    let need_label = !label.is_empty();

    if encoded_len > offload_size {
      let data = Self::offload(packet).await?;
      return self.raw_send_packet(addr, data).await;
    }

    let mut data = vec![0u8; encoded_len];
    packet.encode(&mut data)?;

    if let Some(compress_algo) = compress_algo {
      data = compress_into_vec(compress_algo, &data)?;
    }

    if need_cks {
      let cks = checksum(&cks_algo.unwrap(), &data)?;
    }

    // The send process is as follows:
    //   1. compress if needed
    //   2. checksum if needed
    //   3. encrypt if needed
    //   4. prepend label if needed

    todo!()
  }

  async fn offload(payload: Message<T::Id, T::ResolvedAddress>) -> Result<Bytes, Error<T, D>> {
    todo!()
  }

  async fn raw_send_packet(
    &self,
    addr: &T::ResolvedAddress,
    data: Bytes,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .send_packet(addr, data)
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

  pub(crate) async fn transport_send_packets(
    &self,
    addr: &T::ResolvedAddress,
    packets: TinyVec<Message<T::Id, T::ResolvedAddress>>,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .send_packets(addr, todo!())
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
    let msg = msg.encode_to_bytes()?;
    self
      .inner
      .transport
      .send_message(conn, msg)
      .await
      .map(|_sent| {
        #[cfg(feature = "metrics")]
        {
          metrics::counter!(
            "memberlist.promised.sent",
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
