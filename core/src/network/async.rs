use std::sync::atomic::Ordering;

use crate::{
  delegate::Delegate,
  error::Error,
  // security::{append_bytes, EncryptionAlgo, SecretKey, SecretKeyring, SecurityError},
  transport::TimeoutableStream,
  // types::MessageType,
};

use super::*;
use agnostic::Runtime;
use futures::{future::FutureExt, Future, Stream};
use nodecraft::{resolver::AddressResolver, Node};

mod packet;
mod stream;

// #[cfg(any(test, feature = "test"))]
// pub(crate) mod tests;
// #[cfg(any(test, feature = "test"))]
// pub use tests::*;

impl<D, T> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  pub(crate) async fn send_ping_and_wait_for_ack(
    &self,
    target: &<T::Resolver as AddressResolver>::ResolvedAddress,
    ping: Ping<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    deadline: Duration,
  ) -> Result<bool, Error<T, D>> {
    let mut conn: T::Stream = match self.inner.transport.dial_timeout(target, deadline).await {
      Ok(conn) => conn,
      Err(_) => {
        // If the node is actually dead we expect this to fail, so we
        // shouldn't spam the logs with it. After this point, errors
        // with the connection are real, unexpected errors and should
        // get propagated up.
        return Ok(false);
      }
    };
    if deadline != Duration::ZERO {
      conn.set_timeout(Some(deadline));
    }

    let ping_seq_no = ping.seq_no;
    self.send_message(&mut conn, target, ping.into()).await?;

    let msg: Message<_, _> = self
      .read_message(target, &mut conn)
      .await
      .map(|(_, msg)| msg)?;
    let kind = msg.kind();
    if let Some(ack) = msg.try_unwrap_ack() {
      if ack.seq_no != ping_seq_no {
        return Err(Error::sequence_number_mismatch(ping_seq_no, ack.seq_no));
      }

      Ok(true)
    } else {
      Err(Error::UnexpectedMessage {
        expected: "Ack",
        got: kind,
      })
    }
  }

  /// Used to initiate a push/pull over a stream with a
  /// remote host.
  pub(crate) async fn send_and_receive_state(
    &self,
    node: &Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    join: bool,
  ) -> Result<PushPull<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>, Error<T, D>> {
    // Attempt to connect
    let mut conn = self
      .inner
      .transport
      .dial_timeout(node.address(), self.inner.opts.timeout)
      .await
      .map_err(Error::transport)?;
    tracing::debug!(target:  "showbiz", local_addr = %self.inner.id, peer_addr = %node, "initiating push/pull sync");

    #[cfg(feature = "metrics")]
    {
      metrics::counter!(
        "showbiz.promised.connect",
        self.inner.opts.metric_labels.iter()
      )
      .increment(1);
    }

    // Send our state
    self
      .send_local_state(&mut conn, node.address(), join)
      .await?;

    conn.set_timeout(if self.inner.opts.timeout == Duration::ZERO {
      None
    } else {
      Some(self.inner.opts.timeout)
    });

    match self
      .read_message(node.address(), &mut conn)
      .await
      .map(|(_read, msg)| msg)?
    {
      Message::ErrorResponse(err) => Err(Error::remote(err)),
      Message::PushPull(pp) => Ok(pp),
      msg => Err(Error::unexpected_message("PushPull", msg.kind())),
    }
  }

  pub(crate) async fn transport_send_packet(
    &self,
    addr: &<T::Resolver as AddressResolver>::ResolvedAddress,
    packet: Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .send_packet(addr, packet)
      .await
      .map(|(sent, _)| {
        #[cfg(feature = "metrics")]
        {
          metrics::counter!("showbiz.packet.sent", self.inner.opts.metric_labels.iter())
            .increment(sent as u64);
        }
      })
      .map_err(Error::transport)
  }

  pub(crate) async fn transport_send_packets(
    &self,
    addr: &<T::Resolver as AddressResolver>::ResolvedAddress,
    packet: TinyVec<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .send_packets(addr, packet)
      .await
      .map(|(sent, _)| {
        #[cfg(feature = "metrics")]
        {
          metrics::counter!("showbiz.packet.sent", self.inner.opts.metric_labels.iter())
            .increment(sent as u64);
        }
      })
      .map_err(Error::transport)
  }

  pub(crate) async fn send_message(
    &self,
    conn: &mut T::Stream,
    target: &<T::Resolver as AddressResolver>::ResolvedAddress,
    msg: Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .send_message(conn, target, msg)
      .await
      .map(|sent| {
        #[cfg(feature = "metrics")]
        {
          metrics::counter!(
            "showbiz.promised.sent",
            self.inner.opts.metric_labels.iter()
          )
          .increment(sent as u64);
        }
      })
      .map_err(Error::transport)
  }

  pub(crate) async fn read_message(
    &self,
    from: &<T::Resolver as AddressResolver>::ResolvedAddress,
    conn: &mut T::Stream,
  ) -> Result<
    (
      usize,
      Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    ),
    Error<T, D>,
  > {
    self
      .inner
      .transport
      .read_message(from, conn)
      .await
      .map_err(Error::transport)
  }
}
