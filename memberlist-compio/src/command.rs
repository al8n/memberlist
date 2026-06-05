//! Internal command channel — user-facing Memberlist handle sends;
//! driver task receives and dispatches.

use crate::error::Result;
use bytes::Bytes;
use memberlist_proto::{ChecksumOptions, CompressionOptions, EncryptionOptions, Instant, Node};
use std::{net::SocketAddr, time::Duration};

/// Payload for [`JoinKind::WaitForCompletion`].
pub(crate) struct WaitForCompletionArgs {
  /// Wall-clock instant past which the driver replies with whatever success
  /// count it has accumulated (zero successes surface as `JoinAllFailed`).
  pub(crate) deadline: Instant,
}

/// Semantic of a [`Command::Join`] dispatch.
///
/// The driver routes both kinds through the same start_push_pull
/// fan-out (one outbound exchange per resolved address). The kind
/// only affects WHEN the reply is sent and WHAT count it reports:
/// - `Dispatch`: reply immediately with the number of push/pull
///   exchanges queued (fire-and-forget; the caller does not wait
///   for any exchange to terminate).
/// - `WaitForCompletion`: reply once every dispatched exchange has
///   terminated (`ExchangeCompleted` observed for its `ExchangeId`
///   with `kind == ExchangeKind::PushPull`) OR the deadline elapses,
///   whichever comes first. The reply carries the count of exchanges
///   whose outcome was `ExchangeOutcome::Succeeded`; zero successes
///   surface as `JoinAllFailed`.
pub(crate) enum JoinKind {
  /// Reply immediately with the dispatched-exchange count.
  Dispatch,
  /// Reply once every dispatched exchange has terminated OR the
  /// deadline expires.
  WaitForCompletion(WaitForCompletionArgs),
}

/// Payload for [`Command::Join`].
pub(crate) struct JoinCmd {
  /// Pre-resolved socket addresses of the peers to join.
  pub(crate) addrs: Vec<SocketAddr>,
  /// Dispatch semantic — see [`JoinKind`].
  pub(crate) kind: JoinKind,
  /// One-shot reply channel for the join result.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<usize>>,
}

/// Payload for [`Command::Leave`].
pub(crate) struct LeaveCmd {
  /// One-shot reply channel for the leave result.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<()>>,
}

/// Payload for [`Command::UpdateNodeMetadata`].
pub(crate) struct UpdateNodeMetadataCmd {
  /// New raw metadata bytes for the local node.
  pub(crate) meta: Vec<u8>,
  /// One-shot reply channel for the update result.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<()>>,
}

/// Payload for [`Command::SetCompressionOptions`].
pub(crate) struct SetCompressionOptionsCmd {
  /// New compression configuration to apply in place.
  pub(crate) opts: CompressionOptions,
  /// One-shot reply channel for the reconfiguration result.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<()>>,
}

/// Payload for [`Command::SetChecksumOptions`].
pub(crate) struct SetChecksumOptionsCmd {
  /// New gossip-plane checksum configuration to apply in place.
  pub(crate) opts: ChecksumOptions,
  /// One-shot reply channel for the reconfiguration result.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<()>>,
}

/// Payload for [`Command::SetEncryptionOptions`].
pub(crate) struct SetEncryptionOptionsCmd {
  /// New encryption configuration to apply in place.
  pub(crate) opts: EncryptionOptions,
  /// One-shot reply channel for the reconfiguration result.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<()>>,
}

/// Payload for [`Command::Shutdown`].
pub(crate) struct ShutdownCmd {
  /// One-shot reply channel for the shutdown acknowledgement.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<()>>,
}

/// Payload for [`Command::QueueUserBroadcast`].
pub(crate) struct QueueUserBroadcastCmd {
  /// Application bytes to disseminate cluster-wide via gossip.
  data: Bytes,
  /// One-shot reply channel for the enqueue acknowledgement.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<()>>,
}

impl QueueUserBroadcastCmd {
  /// Construct from the broadcast bytes and a reply channel.
  pub(crate) const fn new(
    data: Bytes,
    reply: futures_channel::oneshot::Sender<Result<()>>,
  ) -> Self {
    Self { data, reply }
  }

  /// The application bytes to broadcast.
  pub(crate) const fn data(&self) -> &Bytes {
    &self.data
  }
}

/// Payload for [`Command::SetLocalState`].
pub(crate) struct SetLocalStateCmd {
  /// Application push/pull local-state snapshot bytes.
  state: Bytes,
  /// One-shot reply channel for the set acknowledgement.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<()>>,
}

impl SetLocalStateCmd {
  /// Construct from the snapshot bytes and a reply channel.
  pub(crate) const fn new(
    state: Bytes,
    reply: futures_channel::oneshot::Sender<Result<()>>,
  ) -> Self {
    Self { state, reply }
  }

  /// The local-state snapshot bytes.
  pub(crate) const fn state(&self) -> &Bytes {
    &self.state
  }
}

/// Payload for [`Command::SetAckPayload`].
pub(crate) struct SetAckPayloadCmd {
  /// Application payload bytes attached to outbound probe acks.
  payload: Bytes,
  /// One-shot reply channel for the set acknowledgement.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<()>>,
}

impl SetAckPayloadCmd {
  /// Construct from the ack-payload bytes and a reply channel.
  pub(crate) const fn new(
    payload: Bytes,
    reply: futures_channel::oneshot::Sender<Result<()>>,
  ) -> Self {
    Self { payload, reply }
  }

  /// The ack-payload bytes.
  pub(crate) const fn payload(&self) -> &Bytes {
    &self.payload
  }
}

/// Payload for [`Command::Ping`].
pub(crate) struct PingCmd<I> {
  /// The node to ping (id + wire address).
  node: Node<I, SocketAddr>,
  /// One-shot reply channel for the round-trip time.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<Duration>>,
}

impl<I> PingCmd<I> {
  /// Construct from a target node and a reply channel.
  pub(crate) const fn new(
    node: Node<I, SocketAddr>,
    reply: futures_channel::oneshot::Sender<Result<Duration>>,
  ) -> Self {
    Self { node, reply }
  }

  /// The node to ping.
  pub(crate) const fn node(&self) -> &Node<I, SocketAddr> {
    &self.node
  }
}

/// Payload for [`Command::SendUser`].
pub(crate) struct SendUserCmd {
  /// Destination wire address.
  to: SocketAddr,
  /// One or more unreliable user-message payloads to direct to `to`.
  payloads: Vec<Bytes>,
  /// One-shot reply channel for the send result.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<()>>,
}

impl SendUserCmd {
  /// Construct from a destination, payloads, and a reply channel.
  pub(crate) fn new(
    to: SocketAddr,
    payloads: Vec<Bytes>,
    reply: futures_channel::oneshot::Sender<Result<()>>,
  ) -> Self {
    Self {
      to,
      payloads,
      reply,
    }
  }

  /// The destination address.
  pub(crate) const fn to(&self) -> &SocketAddr {
    &self.to
  }

  /// The payloads to send.
  pub(crate) fn payloads(&self) -> &[Bytes] {
    &self.payloads
  }
}

/// Payload for [`Command::SendReliable`].
pub(crate) struct SendReliableCmd {
  /// Destination wire address.
  to: SocketAddr,
  /// One or more reliable user-message payloads to deliver to `to`.
  payloads: Vec<Bytes>,
  /// One-shot reply channel for the send result.
  pub(crate) reply: futures_channel::oneshot::Sender<Result<()>>,
}

impl SendReliableCmd {
  /// Construct from a destination, payloads, and a reply channel.
  pub(crate) fn new(
    to: SocketAddr,
    payloads: Vec<Bytes>,
    reply: futures_channel::oneshot::Sender<Result<()>>,
  ) -> Self {
    Self {
      to,
      payloads,
      reply,
    }
  }

  /// The destination address.
  pub(crate) const fn to(&self) -> &SocketAddr {
    &self.to
  }

  /// The payloads to deliver reliably.
  pub(crate) fn payloads(&self) -> &[Bytes] {
    &self.payloads
  }
}

/// Commands sent from the public Memberlist handle to the driver task.
///
/// `I` is the node-id type. All existing variants carry only `SocketAddr` or
/// `Bytes` data and are therefore unaffected by the type parameter; `Ping` is
/// the first variant that carries a full `Node<I, SocketAddr>` and drives the
/// parameterisation.
pub(crate) enum Command<I> {
  /// Join a set of peers (addresses already resolved).
  Join(JoinCmd),
  /// Leave the cluster gracefully.
  Leave(LeaveCmd),
  /// Update the local node's metadata.
  UpdateNodeMetadata(UpdateNodeMetadataCmd),
  /// Update the compression options (in-place reconfiguration).
  SetCompressionOptions(SetCompressionOptionsCmd),
  /// Update the gossip-plane checksum options (in-place reconfiguration).
  SetChecksumOptions(SetChecksumOptionsCmd),
  /// Update the encryption options (in-place reconfiguration).
  SetEncryptionOptions(SetEncryptionOptionsCmd),
  /// Queue an application user-broadcast for cluster-wide gossip.
  QueueUserBroadcast(QueueUserBroadcastCmd),
  /// Set the application push/pull local-state snapshot.
  SetLocalState(SetLocalStateCmd),
  /// Set the application payload attached to outbound probe acks.
  SetAckPayload(SetAckPayloadCmd),
  /// Cleanly shut down the driver task.
  Shutdown(ShutdownCmd),
  /// Ping a specific node and return the round-trip time.
  Ping(PingCmd<I>),
  /// Send one or more unreliable directed user messages.
  SendUser(SendUserCmd),
  /// Send one or more reliable directed user messages.
  SendReliable(SendReliableCmd),
}

#[cfg(test)]
mod tests {
  use super::*;
  use smol_str::SmolStr;

  fn addr() -> SocketAddr {
    "127.0.0.1:7946".parse().unwrap()
  }

  #[test]
  fn queue_user_broadcast_cmd_round_trips_data() {
    let (tx, _rx) = futures_channel::oneshot::channel::<Result<()>>();
    let cmd = QueueUserBroadcastCmd::new(Bytes::from_static(b"hello"), tx);
    assert_eq!(cmd.data(), &Bytes::from_static(b"hello"));
    // The payload survives being wrapped into the dispatch enum.
    let Command::QueueUserBroadcast(c) = Command::<SmolStr>::QueueUserBroadcast(cmd) else {
      panic!("constructed the QueueUserBroadcast variant");
    };
    assert_eq!(c.data(), &Bytes::from_static(b"hello"));
  }

  #[test]
  fn set_local_state_cmd_round_trips_state() {
    let (tx, _rx) = futures_channel::oneshot::channel::<Result<()>>();
    let cmd = SetLocalStateCmd::new(Bytes::from_static(b"snap"), tx);
    assert_eq!(cmd.state(), &Bytes::from_static(b"snap"));
  }

  #[test]
  fn set_ack_payload_cmd_round_trips_payload() {
    let (tx, _rx) = futures_channel::oneshot::channel::<Result<()>>();
    let cmd = SetAckPayloadCmd::new(Bytes::from_static(b"ack"), tx);
    assert_eq!(cmd.payload(), &Bytes::from_static(b"ack"));
  }

  #[test]
  fn ping_cmd_round_trips_node() {
    let (tx, _rx) = futures_channel::oneshot::channel::<Result<Duration>>();
    let node = Node::new(SmolStr::new("peer"), addr());
    let cmd = PingCmd::new(node.clone(), tx);
    assert_eq!(cmd.node(), &node);
  }

  #[test]
  fn send_user_cmd_round_trips_to_and_payloads() {
    let (tx, _rx) = futures_channel::oneshot::channel::<Result<()>>();
    let payloads = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    let cmd = SendUserCmd::new(addr(), payloads.clone(), tx);
    assert_eq!(cmd.to(), &addr());
    assert_eq!(cmd.payloads(), payloads.as_slice());
  }

  #[test]
  fn send_reliable_cmd_round_trips_to_and_payloads() {
    let (tx, _rx) = futures_channel::oneshot::channel::<Result<()>>();
    let payloads = vec![Bytes::from_static(b"x")];
    let cmd = SendReliableCmd::new(addr(), payloads.clone(), tx);
    assert_eq!(cmd.to(), &addr());
    assert_eq!(cmd.payloads(), payloads.as_slice());
  }

  // Each `Command<I>` variant is constructible and routes its payload; the
  // match also exercises the variant tags the driver dispatches on.
  #[test]
  fn command_variants_construct_and_match() {
    let mk_unit = || futures_channel::oneshot::channel::<Result<()>>().0;

    let join = {
      let (tx, _rx) = futures_channel::oneshot::channel::<Result<usize>>();
      Command::<SmolStr>::Join(JoinCmd {
        addrs: vec![addr()],
        kind: JoinKind::Dispatch,
        reply: tx,
      })
    };
    assert!(matches!(join, Command::Join(_)));

    // The WaitForCompletion kind carries a deadline payload.
    let wait_join = {
      let (tx, _rx) = futures_channel::oneshot::channel::<Result<usize>>();
      Command::<SmolStr>::Join(JoinCmd {
        addrs: Vec::new(),
        kind: JoinKind::WaitForCompletion(WaitForCompletionArgs {
          deadline: Instant::now(),
        }),
        reply: tx,
      })
    };
    assert!(matches!(
      wait_join,
      Command::Join(JoinCmd {
        kind: JoinKind::WaitForCompletion(_),
        ..
      })
    ));

    let leave = Command::<SmolStr>::Leave(LeaveCmd { reply: mk_unit() });
    assert!(matches!(leave, Command::Leave(_)));

    let update_meta = Command::<SmolStr>::UpdateNodeMetadata(UpdateNodeMetadataCmd {
      meta: vec![1, 2, 3],
      reply: mk_unit(),
    });
    assert!(matches!(update_meta, Command::UpdateNodeMetadata(_)));

    let set_comp = Command::<SmolStr>::SetCompressionOptions(SetCompressionOptionsCmd {
      opts: CompressionOptions::new(),
      reply: mk_unit(),
    });
    assert!(matches!(set_comp, Command::SetCompressionOptions(_)));

    let set_checksum = Command::<SmolStr>::SetChecksumOptions(SetChecksumOptionsCmd {
      opts: ChecksumOptions::new(),
      reply: mk_unit(),
    });
    assert!(matches!(set_checksum, Command::SetChecksumOptions(_)));

    let set_enc = Command::<SmolStr>::SetEncryptionOptions(SetEncryptionOptionsCmd {
      opts: EncryptionOptions::new(),
      reply: mk_unit(),
    });
    assert!(matches!(set_enc, Command::SetEncryptionOptions(_)));

    let shutdown = Command::<SmolStr>::Shutdown(ShutdownCmd { reply: mk_unit() });
    assert!(matches!(shutdown, Command::Shutdown(_)));

    let queue = Command::<SmolStr>::QueueUserBroadcast(QueueUserBroadcastCmd::new(
      Bytes::from_static(b"u"),
      mk_unit(),
    ));
    assert!(matches!(queue, Command::QueueUserBroadcast(_)));

    let local_state =
      Command::<SmolStr>::SetLocalState(SetLocalStateCmd::new(Bytes::from_static(b"s"), mk_unit()));
    assert!(matches!(local_state, Command::SetLocalState(_)));

    let ack =
      Command::<SmolStr>::SetAckPayload(SetAckPayloadCmd::new(Bytes::from_static(b"a"), mk_unit()));
    assert!(matches!(ack, Command::SetAckPayload(_)));

    let ping = {
      let (tx, _rx) = futures_channel::oneshot::channel::<Result<Duration>>();
      Command::<SmolStr>::Ping(PingCmd::new(Node::new(SmolStr::new("p"), addr()), tx))
    };
    assert!(matches!(ping, Command::Ping(_)));

    let send_user = Command::<SmolStr>::SendUser(SendUserCmd::new(
      addr(),
      vec![Bytes::from_static(b"x")],
      mk_unit(),
    ));
    assert!(matches!(send_user, Command::SendUser(_)));

    let send_reliable = Command::<SmolStr>::SendReliable(SendReliableCmd::new(
      addr(),
      vec![Bytes::from_static(b"y")],
      mk_unit(),
    ));
    assert!(matches!(send_reliable, Command::SendReliable(_)));
  }
}
