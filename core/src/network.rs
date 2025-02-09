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

  /// Used to initiate a push/pull over a stream with a
  /// remote host.
  #[allow(clippy::blocks_in_conditions)]
  pub(crate) async fn send_and_receive_state(
    &self,
    node: &Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    join: bool,
  ) -> Result<PushPull<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>, Error<T, D>> {
    // Attempt to connect
    let mut conn = self
      .inner
      .transport
      .dial_with_deadline(
        node.address(),
        <T::Runtime as RuntimeLite>::now() + self.inner.opts.timeout,
      )
      .await
      .map_err(Error::transport)?;
    tracing::debug!(local_addr = %self.inner.id, peer_addr = %node, "memberlist: initiating push/pull sync");

    #[cfg(feature = "metrics")]
    {
      metrics::counter!(
        "memberlist.promised.connect",
        self.inner.opts.metric_labels.iter()
      )
      .increment(1);
    }

    // Send our state
    self.send_local_state(&mut conn, join).await?;

    conn.set_deadline(Some(
      <T::Runtime as RuntimeLite>::now() + self.inner.opts.timeout,
    ));

    let (_, payload) = self.read_message(node.address(), &mut conn).await?;

    let (_, msg) = <MessageRef::<'_, <T::Id as Data>::Ref<'_>, <T::ResolvedAddress as Data>::Ref<'_>> as DataRef<Message<T::Id, T::ResolvedAddress>>>::decode(&payload)?;
    match msg {
      MessageRef::ErrorResponse(resp) => {
        let resp = <ErrorResponse as Data>::from_ref(resp)?;
        tracing::error!(local_addr = %self.inner.id, peer_addr = %node, err = %resp, "memberlist: push/pull sync failed");
        return Err(Error::remote(resp));
      }
      MessageRef::PushPull(pp) => {
        if let Err(e) = self
          .inner
          .transport
          .cache_stream(node.address(), conn)
          .await
        {
          tracing::debug!(local_addr = %self.inner.id, peer_addr = %node, err = %e, "memberlist.transport: failed to cache stream");
        }
        Ok(todo!())
      }
      msg => Err(Error::unexpected_message("PushPull", msg.ty().kind())),
    }
  }

  pub(crate) async fn transport_send_packet(
    &self,
    addr: &T::ResolvedAddress,
    packet: Bytes,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .send_packet(addr, packet)
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
    packet: Bytes,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .send_packets(addr, packet)
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
