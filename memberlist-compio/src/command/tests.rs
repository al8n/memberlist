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
    let (tx, _rx) = futures_channel::oneshot::channel::<JoinReply>();
    Command::<SmolStr>::Join(JoinCmd {
      addrs: vec![addr()],
      kind: JoinKind::Dispatch,
      reply: tx,
    })
  };
  assert!(matches!(join, Command::Join(_)));

  // The WaitForCompletion kind carries a deadline payload.
  let wait_join = {
    let (tx, _rx) = futures_channel::oneshot::channel::<JoinReply>();
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

  #[cfg(compression)]
  let set_comp = Command::<SmolStr>::SetCompressionOptions(SetCompressionOptionsCmd {
    opts: CompressionOptions::new(),
    reply: mk_unit(),
  });
  #[cfg(compression)]
  assert!(matches!(set_comp, Command::SetCompressionOptions(_)));

  #[cfg(checksum)]
  let set_checksum = Command::<SmolStr>::SetChecksumOptions(SetChecksumOptionsCmd {
    opts: ChecksumOptions::new(),
    reply: mk_unit(),
  });
  #[cfg(checksum)]
  assert!(matches!(set_checksum, Command::SetChecksumOptions(_)));

  #[cfg(encryption)]
  let set_enc = Command::<SmolStr>::SetEncryptionOptions(SetEncryptionOptionsCmd {
    opts: EncryptionOptions::new(),
    reply: mk_unit(),
  });
  #[cfg(encryption)]
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
