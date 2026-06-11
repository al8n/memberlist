//! The QUIC backend driver: a quinn-style `Future::poll` pump that solely owns a
//! [`QuicEndpoint`], its UDP socket, and the periodic schedulers, advancing the
//! membership machine and republishing the [`MemberlistSnapshot`].

use std::{
  collections::{HashMap, HashSet, VecDeque},
  future::Future,
  net::SocketAddr,
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  task::{Context, Poll},
  time::Duration,
};

use agnostic::{
  Runtime,
  net::{Net, UdpSocket},
};
use bytes::Bytes;
use flume::Sender;
use memberlist_proto::{
  DatagramSendOutcome, Instant, PingId, QuicEndpoint, UnreliableTransport,
  codec::{
    DecodeOptions, EncodeOptions, decode_incoming, encode_outgoing, encode_outgoing_compound,
    parse_messages,
  },
  event::{Event, ExchangeId, ExchangeKind, ExchangeOutcome, PushPullKind, Transmit},
};

use crate::{
  NodeId,
  cidr::{CidrFilter, cidr_blocks},
  command::{
    Command, JoinCmd, LeaveCmd, PingCmd, QueueUserBroadcastCmd, SendReliableCmd, SendUserCmd,
    SetAckPayloadCmd, SetChecksumOptionsCmd, SetCompressionOptionsCmd, SetEncryptionOptionsCmd,
    SetLocalStateCmd, ShutdownCmd, UpdateNodeMetadataCmd,
  },
  error::Error,
  observation::observation_payload_bytes,
  shared::Shared,
  snapshot::snapshot_of,
};

/// IP-layer UDP payload maximum; caps the per-recv buffer.
const GOSSIP_RECV_BUF_MAX: usize = 65507;

/// Cap on the count of application-data events retained after a full observation
/// channel (the payload byte budget bounds their bytes; this bounds their count).
const OBS_OVERFLOW_MAX: usize = 1024;

/// A `WaitForCompletion` join awaiting its dispatched push/pull exchanges. The
/// machine emits one `ExchangeCompleted` per dispatched exchange (success or
/// `stream_timeout` failure), so the set always drains — no deadline needed.
struct PendingJoin {
  pending: HashSet<ExchangeId>,
  contacted: usize,
  requested: usize,
  reply: futures_channel::oneshot::Sender<Result<usize, Error>>,
}

/// An in-flight graceful leave; every joined caller's reply resolves together
/// when `LeftCluster` fires.
struct PendingLeave {
  repliers: Vec<futures_channel::oneshot::Sender<Result<(), Error>>>,
}

/// An outstanding application-ping call; resolved on `PingCompleted` (reply
/// `Ok(rtt)`) or `PingFailed` (reply `Err(PingTimeout)`) via the matching
/// `PingId`. On driver exit, drained with `Err(Shutdown)`.
struct PendingPing {
  ping_id: PingId,
  reply: futures_channel::oneshot::Sender<Result<Duration, Error>>,
}

/// An outstanding reliable directed-send call; resolved on
/// `ExchangeCompleted(UserMessage)` as each tracked `ExchangeId` surfaces.
/// For QUIC, `QuicEndpoint::start_user_message` returns a `StreamId` that
/// coerces directly to the `ExchangeId` stamped on the completion event —
/// no `poll_action` drain is needed. On driver exit, drained with
/// `Err(Shutdown)`.
struct PendingUserSend {
  pending: HashSet<ExchangeId>,
  failed: usize,
  reply: futures_channel::oneshot::Sender<Result<(), Error>>,
}

/// The single-owner QUIC driver future. Runs until shutdown (the last handle
/// dropped, or a `Shutdown` command).
pub(crate) struct QuicDriver<I: NodeId, R: Runtime> {
  endpoint: QuicEndpoint<I>,
  /// The shared UDP socket carrying QUIC packets (and gossip on the UDP-fallback
  /// path). Wrapped in `Option` so the shutdown branch can drop it (releasing the
  /// bound port) BEFORE acking the shutdown caller; it is `Some` for the whole
  /// running lifetime and only taken during teardown.
  socket: Option<<R::Net as Net>::UdpSocket>,
  shared: Arc<Shared<I>>,
  /// The endpoint snapshot version last published to `shared`; the snapshot is
  /// republished only when it differs (see the stream driver for the rationale).
  last_snapshot_version: u64,
  /// The load-shedding counters last published to `shared` (republished on change).
  last_metrics: memberlist_proto::metrics::Metrics,
  /// Hand-off to the observation task (delegate dispatch + event-stream fan-out).
  obs_tx: Sender<Event<I, SocketAddr>>,
  /// Cluster label threaded into the gossip `EncodeOptions` / `DecodeOptions` so
  /// outbound gossip is stamped and inbound gossip is verified against the same
  /// label.
  pub(crate) label: Option<bytes::Bytes>,
  /// Outstanding synchronous joins, reduced as their exchanges complete.
  pending_joins: HashMap<u64, PendingJoin>,
  /// Monotonic key source for `pending_joins`.
  next_pending_join_id: u64,
  /// The in-flight graceful leave, resolved on `LeftCluster`.
  pending_leave: Option<PendingLeave>,
  /// Outstanding application-ping calls; resolved via `PingId` correlation.
  pending_pings: Vec<PendingPing>,
  /// Outstanding reliable-send calls; resolved when all tracked exchange ids
  /// surface a terminal `ExchangeCompleted(UserMessage)`.
  pending_user_sends: Vec<PendingUserSend>,
  /// The parked replies of `Shutdown` commands. A reply is NOT sent inline at
  /// dispatch: every caller is parked here and acked only after the shutdown
  /// branch drops the UDP socket, so the bound port is free when each caller
  /// resumes from `shutdown().await` and an immediate rebind on the same address
  /// succeeds. A `Vec` because several callers can race `shutdown()` concurrently
  /// — each must get its own ack, and none before the socket drop.
  shutdown_reply: Vec<futures_channel::oneshot::Sender<Result<(), Error>>>,
  /// Bytes of payload-bearing events queued in `obs_tx` (added on enqueue,
  /// subtracted by the obs task on dequeue) — the byte backstop's counter.
  obs_payload_bytes: Arc<AtomicU64>,
  /// Queued-payload byte budget: `Some(4 * max_stream_frame_size)` on a bounded
  /// obs channel, `None` (no byte cap) on an unbounded one.
  obs_payload_budget: Option<u64>,
  /// Application-data events retained after a full obs channel, retried on a
  /// later poll rather than dropped (bounded by the payload byte budget and
  /// `OBS_OVERFLOW_MAX`).
  obs_overflow: VecDeque<Event<I, SocketAddr>>,
  recv_buf: Vec<u8>,
  recv_batch: usize,
  /// Per-poll cap on each drained surface (bounds the work one poll performs;
  /// remaining work triggers a self-wake).
  transmit_batch: usize,
  timer: Option<Pin<Box<R::Sleep>>>,
  timer_deadline: Option<Instant>,
  idle_wake: Duration,
  /// CIDR transport-source filter: a UDP packet (QUIC handshake or gossip
  /// datagram) from a blocked source IP is dropped before the machine sees it, so
  /// a blocked peer forms no connection. `()` when the `cidr` feature is off.
  cidr_policy: CidrFilter,
}

impl<I: NodeId, R: Runtime> QuicDriver<I, R> {
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn new(
    endpoint: QuicEndpoint<I>,
    socket: <R::Net as Net>::UdpSocket,
    shared: Arc<Shared<I>>,
    recv_batch: usize,
    transmit_batch: usize,
    obs_tx: Sender<Event<I, SocketAddr>>,
    obs_payload_bytes: Arc<AtomicU64>,
    obs_payload_budget: Option<u64>,
    label: Option<Bytes>,
    cidr_policy: CidrFilter,
  ) -> Self {
    let buf_len = endpoint
      .gossip_mtu()
      .saturating_add(memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD)
      .saturating_add(memberlist_proto::CHECKSUMED_WRAPPER_OVERHEAD)
      .min(GOSSIP_RECV_BUF_MAX);
    let last_snapshot_version = endpoint.endpoint_ref().snapshot_version();
    Self {
      endpoint,
      socket: Some(socket),
      shared,
      last_snapshot_version,
      last_metrics: memberlist_proto::metrics::Metrics::default(),
      obs_tx,
      label,
      pending_joins: HashMap::new(),
      next_pending_join_id: 0,
      pending_leave: None,
      pending_pings: Vec::new(),
      pending_user_sends: Vec::new(),
      shutdown_reply: Vec::new(),
      obs_payload_bytes,
      obs_payload_budget,
      obs_overflow: VecDeque::new(),
      recv_buf: vec![0u8; buf_len],
      // Clamp to at least 1: a 0 batch makes the `recv_n < recv_batch` loop never
      // run (no gossip ever received) and `recv_n == recv_batch` self-wake forever.
      recv_batch: recv_batch.max(1),
      transmit_batch,
      timer: None,
      timer_deadline: None,
      idle_wake: Duration::from_secs(1),
      cidr_policy,
    }
  }

  /// Applies one command to the machine and replies on its oneshot.
  fn dispatch(&mut self, cmd: Command<I>, now: Instant) {
    match cmd {
      Command::Join(JoinCmd { addrs, wait, reply }) => {
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err(Error::NotRunning));
          return;
        }
        if !wait {
          // Dispatch: fire-and-forget; reply the dispatched count.
          let mut count = 0usize;
          for addr in &addrs {
            // Skip an outbound dial to a CIDR-blocked seed: starting the QUIC
            // push/pull would open a connection and emit handshake packets to a
            // peer our own policy excludes. The blocked seed is not contacted.
            if cidr_blocks(&self.cidr_policy, addr.ip()) {
              continue;
            }
            // Ignoring StreamId: per-seed outcome surfaces via poll_event.
            let _ = self
              .endpoint
              .start_push_pull(*addr, PushPullKind::Join, now);
            count += 1;
          }
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Ok(count));
          return;
        }
        // WaitForCompletion: track each dispatched exchange; account_event
        // replies the contacted count once they all complete.
        let mut pending = HashSet::with_capacity(addrs.len());
        let mut blocked = 0usize;
        for addr in &addrs {
          // Skip a CIDR-blocked seed (see the non-wait arm): no QUIC handshake is
          // emitted to a peer our own policy excludes.
          if cidr_blocks(&self.cidr_policy, addr.ip()) {
            blocked += 1;
            continue;
          }
          let sid = self
            .endpoint
            .start_push_pull(*addr, PushPullKind::Join, now);
          pending.insert(ExchangeId::from(sid));
        }
        if pending.is_empty() {
          // No exchange to wait on. If some seeds were CIDR-blocked, the join
          // contacted none of them — a bounded failure mirroring the stream
          // driver; an empty (or fully blocked) seed set otherwise replies Ok(0).
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(if blocked > 0 {
            Err(Error::JoinFailed(addrs.len()))
          } else {
            Ok(0)
          });
          return;
        }
        let requested = pending.len();
        let id = self.next_pending_join_id;
        self.next_pending_join_id = self.next_pending_join_id.wrapping_add(1);
        self.pending_joins.insert(
          id,
          PendingJoin {
            pending,
            contacted: 0,
            requested,
            reply,
          },
        );
      }
      Command::Leave(LeaveCmd { reply }) => {
        // A second leave racing an in-flight one joins it (both replies resolve
        // together on the single LeftCluster); re-invoking leave once
        // Leaving/Left emits no second completion, so a fresh waiter would hang.
        if let Some(pl) = self.pending_leave.as_mut() {
          pl.repliers.push(reply);
          return;
        }
        let was_running = self.endpoint.is_running();
        let res = self
          .endpoint
          .leave(now)
          .map_err(|e| Error::Io(std::io::Error::other(e.to_string())));
        match res {
          // A running leave queues the Dead-self notices and WILL emit
          // LeftCluster once they drain; park until then, so Ok means the leave
          // reached the wire.
          Ok(()) if was_running => {
            self.pending_leave = Some(PendingLeave {
              repliers: vec![reply],
            });
          }
          // A no-op (not running) or an error fires no completion — reply now.
          other => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.send(other);
          }
        }
      }
      Command::Shutdown(ShutdownCmd { reply }) => {
        // Do NOT ack inline: the UDP socket is still bound here. Flag shutdown
        // and park the reply; the shutdown branch acks every parked caller only
        // AFTER it drops the socket, so an immediate rebind on the same address
        // after `shutdown().await` succeeds.
        self.shared.begin_shutdown();
        self.shutdown_reply.push(reply);
      }
      Command::Ping(PingCmd { node, reply }) => {
        // Gate on a running node: after `leave()` the probe scheduler is
        // stopped, so a new application ping's completion event would never
        // arrive and the caller would hang forever. Reject with `NotRunning`.
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err(Error::NotRunning));
          return;
        }
        let ping_id = self.endpoint.ping(node, now).expect("issued while running");
        self.pending_pings.push(PendingPing { ping_id, reply });
      }
      Command::SendUser(SendUserCmd {
        to,
        payloads,
        reply,
      }) => {
        // Gate on a running node: after `leave()` the gossip scheduler is
        // stopped; reject immediately.
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err(Error::NotRunning));
          return;
        }
        if cidr_blocks(&self.cidr_policy, to.ip()) {
          // Our own policy excludes the destination: do not emit an unreliable
          // user datagram to a blocked peer.
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err(Error::SendFailed));
          return;
        }
        let res = self
          .endpoint
          .send_user_packets(to, &payloads)
          .map_err(|e| Error::PayloadTooLarge(e.to_string()));
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::SendReliable(SendReliableCmd {
        to,
        payloads,
        reply,
      }) => {
        // Gate on a running node: after `leave()` the QUIC stream coordinator
        // is stopping; a new `start_user_message` would never produce a bridge
        // and the caller would hang. Reject with `NotRunning`.
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err(Error::NotRunning));
          return;
        }
        if payloads.is_empty() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Ok(()));
          return;
        }
        if cidr_blocks(&self.cidr_policy, to.ip()) {
          // Our own policy excludes the destination: fail the reliable send at the
          // transport boundary without opening a QUIC stream (no handshake packets
          // are emitted to a blocked peer).
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err(Error::SendFailed));
          return;
        }
        let mut pending = HashSet::with_capacity(payloads.len());
        for payload in payloads {
          // `QuicEndpoint::start_user_message` calls `service_dials` +
          // `flush_outbound` in-band (no separate `poll_action` loop needed,
          // unlike the stream driver). The returned `StreamId` coerces to the
          // `ExchangeId` that the bridge-reap path stamps on
          // `Event::ExchangeCompleted(UserMessage)`.
          // Ignoring StreamId: only ExchangeId::from is needed for correlation.
          let stream_id = self
            .endpoint
            .start_user_message(to, payload, now)
            .expect("issued while running");
          pending.insert(ExchangeId::from(stream_id));
        }
        self.pending_user_sends.push(PendingUserSend {
          pending,
          failed: 0,
          reply,
        });
      }
      Command::SetCompressionOptions(SetCompressionOptionsCmd { opts, reply }) => {
        // Gate on a running node: after `leave()` the endpoint emits no
        // protocol traffic, so a new compression policy could never take
        // effect on the wire. Reject with `NotRunning` rather than ack a
        // change that will never be observed.
        let res = if self.endpoint.is_running() {
          self.endpoint.set_compression_options(opts);
          Ok(())
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::SetChecksumOptions(SetChecksumOptionsCmd { opts, reply }) => {
        // Gate on a running node FIRST: after `leave()` the endpoint emits no
        // gossip datagrams, so a new checksum policy (a gossip-plane concern)
        // could never take effect on the wire. When running, validate the
        // policy before applying it: an algorithm whose backend feature is
        // absent is accepted by the options builder, but every later
        // `checksum_gossip` would fail and the driver would drop the datagram —
        // so a "successful" change would silently disable ALL gossip after a
        // false `Ok`.
        let res = if self.endpoint.is_running() {
          self
            .endpoint
            .set_checksum_options(opts)
            .map_err(Error::Checksum)
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::SetEncryptionOptions(SetEncryptionOptionsCmd { opts, reply }) => {
        // Gate on a running node FIRST: after `leave()` the endpoint emits
        // no protocol traffic, so a new encryption policy could never take
        // effect on the wire. When running, validate the policy before
        // applying it: a keyring naming an unsupported AEAD would silently
        // break the cluster after a false `Ok`.
        let res = if self.endpoint.is_running() {
          match crate::transform::validate_encryption(&opts) {
            Ok(()) => {
              self.endpoint.set_encryption_options(opts);
              Ok(())
            }
            Err(e) => Err(e),
          }
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::UpdateNodeMetadata(UpdateNodeMetadataCmd { meta, reply }) => {
        // Gate on a running node: after `leave()` the schedulers are stopped, so
        // a metadata change could never be gossiped. Build the validated `Meta`
        // (rejecting an over-cap value) before applying.
        let res = if self.endpoint.is_running() {
          match memberlist_proto::typed::Meta::try_from(meta) {
            Ok(m) => self
              .endpoint
              .update_meta(m)
              .map_err(|e| Error::PayloadTooLarge(e.to_string())),
            Err(e) => Err(Error::PayloadTooLarge(e.to_string())),
          }
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::QueueUserBroadcast(QueueUserBroadcastCmd { data, reply }) => {
        // Gate on a running node FIRST: after `leave()` the gossip scheduler is
        // stopped, so the broadcast would never drain. The machine setter rejects
        // an over-MTU lone datagram without storing it.
        let res = if self.endpoint.is_running() {
          self
            .endpoint
            .queue_user_broadcast(data)
            .map_err(|e| Error::PayloadTooLarge(e.to_string()))
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::SetLocalState(SetLocalStateCmd { state, reply }) => {
        // Gate on a running node FIRST: after `leave()` no push/pull exchange
        // will carry the snapshot. The machine setter rejects a snapshot whose
        // framed PushPull exceeds the stream frame budget.
        let res = if self.endpoint.is_running() {
          self
            .endpoint
            .set_local_state_snapshot(state)
            .map_err(|e| Error::PayloadTooLarge(e.to_string()))
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::SetAckPayload(SetAckPayloadCmd { payload, reply }) => {
        // Gate on a running node FIRST: after `leave()` no probe ack will carry
        // the payload. The machine setter rejects an over-budget ack without
        // storing it (an over-budget ack always fails to send, so a probing peer
        // would otherwise falsely suspect this node).
        let res = if self.endpoint.is_running() {
          self
            .endpoint
            .set_ack_payload(payload)
            .map_err(|e| Error::PayloadTooLarge(e.to_string()))
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
    }
  }

  /// Synchronous-command accounting for a surfaced event: reduces the matching
  /// `WaitForCompletion` join on a push/pull `ExchangeCompleted` (replying when
  /// its set empties), and resolves a parked leave on `LeftCluster`.
  fn account_event(&mut self, ev: &Event<I, SocketAddr>) {
    match ev {
      Event::ExchangeCompleted(p) if p.kind() == ExchangeKind::PushPull => {
        let eid = p.eid();
        let succeeded = matches!(p.outcome(), ExchangeOutcome::Succeeded);
        let mut completed = None;
        for (key, pj) in self.pending_joins.iter_mut() {
          if pj.pending.remove(&eid) {
            if succeeded {
              pj.contacted += 1;
            }
            if pj.pending.is_empty() {
              completed = Some(*key);
            }
            break;
          }
        }
        if let Some(key) = completed
          && let Some(pj) = self.pending_joins.remove(&key)
        {
          let res = if pj.contacted == 0 {
            Err(Error::JoinFailed(pj.requested))
          } else {
            Ok(pj.contacted)
          };
          // Ignoring Err: the join caller dropped its reply receiver.
          let _ = pj.reply.send(res);
        }
      }
      Event::LeftCluster => {
        if let Some(pl) = self.pending_leave.take() {
          for replier in pl.repliers {
            // Ignoring Err: a leave caller dropped its reply receiver.
            let _ = replier.send(Ok(()));
          }
        }
      }
      // Ping completion: resolve the matching waiter with the observed RTT.
      // The event still flows to the observation task (additive correlation —
      // `PingCompleted` also fires the delegate's `notify_ping_complete`).
      Event::PingCompleted(p) => {
        let pid = p.ping_id();
        if let Some(idx) = self.pending_pings.iter().position(|pp| pp.ping_id == pid) {
          let pp = self.pending_pings.swap_remove(idx);
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = pp.reply.send(Ok(p.rtt()));
        }
      }
      // Ping failure: resolve the matching waiter with `PingTimeout`. Same
      // additive semantics as `PingCompleted`.
      Event::PingFailed(p) => {
        let pid = p.ping_id();
        if let Some(idx) = self.pending_pings.iter().position(|pp| pp.ping_id == pid) {
          let pp = self.pending_pings.swap_remove(idx);
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = pp.reply.send(Err(Error::PingTimeout));
        }
      }
      // Reliable user-send completion: reduce the pending set; reply when empty.
      Event::ExchangeCompleted(p) if p.kind() == ExchangeKind::UserMessage => {
        let eid = p.eid();
        let failed = matches!(p.outcome(), ExchangeOutcome::Failed);
        if let Some(idx) = self
          .pending_user_sends
          .iter()
          .position(|ps| ps.pending.contains(&eid))
        {
          let ps = &mut self.pending_user_sends[idx];
          ps.pending.remove(&eid);
          if failed {
            ps.failed += 1;
          }
          if ps.pending.is_empty() {
            let ps = self.pending_user_sends.swap_remove(idx);
            let res = if ps.failed > 0 {
              Err(Error::SendFailed)
            } else {
              Ok(())
            };
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = ps.reply.send(res);
          }
        }
      }
      _ => {}
    }
  }

  /// Drains each machine surface up to `transmit_batch` items in one pass.
  /// Returns `(worked, more)`: whether any surface produced work (republish the
  /// snapshot) and whether any surface hit its cap with work left (self-wake).
  fn drain_surfaces(&mut self, cx: &mut Context<'_>) -> (bool, bool) {
    let decode_opts = DecodeOptions::new(self.label.clone());
    let encode_opts = EncodeOptions::new(self.label.clone());
    let budget = self.transmit_batch.max(1);
    let now = Instant::now();
    let mut worked = false;
    let mut more = false;

    // Inbound gossip: decrypt, strip label, decode, feed each message back.
    // Drain to EMPTY (not budgeted): the poll loop advances membership time
    // right after `drain_surfaces`, and a datagram-carried Ack must be decoded
    // and applied before a probe deadline can fire (a QUIC packet's handle_udp
    // queues datagram payloads here without advancing membership time). Bounded
    // by the proto mem_ingress cap, so a flood cannot make this loop unbounded.
    //
    // The transform runs inline on the pump (as on every plane): the per-poll
    // work is bounded by the mem_ingress drain (itself capped), and the
    // microsecond-scale transform is negligible against the probe FSM's absolute
    // failure_deadline.
    let mut ingress = 0;
    while let Some((from, raw)) = self.endpoint.poll_memberlist_ingress() {
      ingress += 1;
      let plain = match self.endpoint.decrypt_gossip(&raw) {
        Ok(p) => Bytes::from(p),
        Err(_) => continue,
      };
      let inner = match decode_incoming(plain, &decode_opts) {
        Ok(b) => b,
        Err(_) => continue,
      };
      let msgs = match parse_messages::<I, SocketAddr>(inner) {
        Ok(m) => m,
        Err(_) => continue,
      };
      for msg in msgs {
        self.endpoint.handle_packet(from, msg, now);
      }
    }
    worked |= ingress > 0;

    // Outbound gossip: encode (plain or compound), compress, encrypt, and route
    // onto the unreliable wire the endpoint is configured for: a QUIC datagram
    // over the peer's pooled connection (`UnreliableTransport::Datagram`, the
    // default) or the shared UDP socket (`UnreliableTransport::Udp`).
    // `encode_outgoing*` only fails on a typed↔buffa bridge error that
    // round-trips a locally-built message; matches the stream driver's
    // drop-on-codec-fail policy. An empty encryption config makes
    // `encrypt_gossip` a copy; a transient `send_to` error (ENOBUFS / ICMP
    // unreachable surfacing as a syscall error) is non-fatal per the gossip
    // drop discipline.
    let mut needs_flush = false;
    let mut sent = 0;
    while sent < budget {
      let Some(transmit) = self.endpoint.poll_memberlist_transmit() else {
        break;
      };
      sent += 1;
      let (peer, plain) = match transmit {
        Transmit::Packet(pkt) => {
          let (to, msg) = pkt.into_parts();
          match encode_outgoing(&msg, &encode_opts) {
            Ok(b) => (to, b),
            Err(_) => continue,
          }
        }
        Transmit::Compound(cmp) => {
          let (to, msgs) = cmp.into_parts();
          match encode_outgoing_compound(&msgs, &encode_opts) {
            Ok(b) => (to, b),
            Err(_) => continue,
          }
        }
      };
      let compressed = self.endpoint.compress_gossip(&plain);
      let checksummed = match self.endpoint.checksum_gossip(&compressed) {
        Ok(b) => b,
        // Checksum configured but its backend was not built in — drop rather
        // than emit an unverifiable datagram on a checksum-configured path.
        Err(_) => continue,
      };
      // `Bytes` so the datagram-queue path and the UDP fallback can share the
      // same encoded payload without a second copy (`clone` is an O(1) refcount
      // bump).
      let on_wire = match self.endpoint.encrypt_gossip(&checksummed) {
        Ok(b) => Bytes::from(b),
        Err(_) => continue,
      };
      match self.endpoint.unreliable_transport() {
        UnreliableTransport::Udp => {
          if let Some(socket) = self.socket.as_ref() {
            // Ignoring Poll: gossip is best-effort — a full or errored UDP send
            // drops the datagram and SWIM recovers on the next round.
            let _ = socket.poll_send_to(cx, &on_wire, peer);
          }
        }
        UnreliableTransport::Datagram => {
          match self
            .endpoint
            .queue_unreliable_datagram(peer, on_wire.clone(), now)
          {
            DatagramSendOutcome::Queued => needs_flush = true,
            // NotReady may mean queue_unreliable_datagram just initiated a cold
            // dial; flush this tick so the connection's Initial is emitted now
            // (else the connection does not warm until the next driver wake). The
            // gossip itself still goes out immediately over the UDP fallback.
            DatagramSendOutcome::NotReady => {
              needs_flush = true;
              if let Some(socket) = self.socket.as_ref() {
                // Ignoring Poll: a transient UDP send error is non-fatal — gossip
                // is lossy and the next probe/gossip round recovers.
                let _ = socket.poll_send_to(cx, &on_wire, peer);
              }
            }
            // TooLarge: the connection is already Established (max_size was Some),
            // so there is no pending Initial to flush; just fall back to UDP.
            DatagramSendOutcome::TooLarge => {
              if let Some(socket) = self.socket.as_ref() {
                // Ignoring Poll: a transient UDP send error is non-fatal — gossip
                // is lossy and the next probe/gossip round recovers.
                let _ = socket.poll_send_to(cx, &on_wire, peer);
              }
            }
          }
        }
      }
    }
    worked |= sent > 0;
    more |= sent == budget;

    // Flush any datagrams queued above into `out` THIS poll so the raw-QUIC
    // loop below sends them now — a datagram-borne probe whose timeout is armed
    // this same tick must not wait for the next driver wake (that wake can be
    // the timeout).
    if needs_flush {
      self.endpoint.flush_outbound_transmits(now);
    }

    // Raw QUIC datagrams: already wire-framed by quinn-proto, no codec wrap.
    let mut raw_sent = 0;
    while raw_sent < budget {
      let Some((dest, bytes)) = self.endpoint.poll_transmit() else {
        break;
      };
      raw_sent += 1;
      if let Some(socket) = self.socket.as_ref() {
        // Ignoring Poll: a dropped QUIC datagram is retransmitted by quinn-proto.
        let _ = socket.poll_send_to(cx, &bytes, dest);
      }
    }
    worked |= raw_sent > 0;
    more |= raw_sent == budget;

    // Observation events: retry the overflow first, then drain up to the budget.
    self.flush_obs_overflow();
    let mut events = 0;
    while events < budget {
      let Some(ev) = self.endpoint.poll_event() else {
        break;
      };
      events += 1;
      self.send_observation(ev);
    }
    worked |= events > 0;
    more |= events == budget;

    (worked, more)
  }

  /// Retries retained overflow events into the obs channel, stopping at the first
  /// `Full`. Sent events stay byte-accounted (the obs task subtracts on dequeue).
  fn flush_obs_overflow(&mut self) {
    while let Some(ev) = self.obs_overflow.pop_front() {
      match self.obs_tx.try_send(ev) {
        Ok(()) => {}
        Err(flume::TrySendError::Full(ev)) => {
          self.obs_overflow.push_front(ev);
          break;
        }
        Err(flume::TrySendError::Disconnected(ev)) => {
          // The obs task is gone: reclaim this event's reserved payload bytes.
          if let Some(bytes) = observation_payload_bytes(&ev) {
            self.obs_payload_bytes.fetch_sub(bytes, Ordering::Relaxed);
          }
        }
      }
    }
  }

  /// Hands one event to the obs task. A full channel retains application data for
  /// retry (bounded by the payload byte budget and `OBS_OVERFLOW_MAX`) and drops
  /// recoverable membership/control events, counting them.
  fn send_observation(&mut self, ev: Event<I, SocketAddr>) {
    self.account_event(&ev);
    let payload = observation_payload_bytes(&ev);
    // Byte backstop: refuse a payload event if enqueuing it would push the queued
    // payload bytes over budget — the count cap alone does not bound memory when
    // events carry large reliable payloads.
    if let (Some(budget), Some(bytes)) = (self.obs_payload_budget, payload)
      && self
        .obs_payload_bytes
        .load(Ordering::Relaxed)
        .saturating_add(bytes)
        > budget
    {
      self.shared.add_observation_dropped(1);
      return;
    }
    // Reserve the payload bytes before the event becomes visible to the obs task,
    // so its release (subtract on receive) can never run ahead of the reservation
    // and wrap the counter on a multi-thread runtime.
    if let Some(bytes) = payload {
      self.obs_payload_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    match self.obs_tx.try_send(ev) {
      // Reserved above; the obs task releases it on receive.
      Ok(()) => {}
      Err(flume::TrySendError::Full(ev)) => match payload {
        // Application data the event stream cannot reconstruct: retain (still
        // reserved) for a retry.
        Some(_) if self.obs_overflow.len() < OBS_OVERFLOW_MAX => {
          self.obs_overflow.push_back(ev);
        }
        // Recoverable membership/control, or the overflow is full: drop, count,
        // and roll back any reservation.
        _ => {
          if let Some(bytes) = payload {
            self.obs_payload_bytes.fetch_sub(bytes, Ordering::Relaxed);
          }
          self.shared.add_observation_dropped(1);
        }
      },
      // The obs task is gone: roll back the reservation.
      Err(flume::TrySendError::Disconnected(_)) => {
        if let Some(bytes) = payload {
          self.obs_payload_bytes.fetch_sub(bytes, Ordering::Relaxed);
        }
      }
    }
  }

  /// (Re)arms the wakeup timer for `target` if it is not already armed for it.
  fn arm_timer(&mut self, target: Instant, now: Instant) {
    if self.timer_deadline != Some(target) {
      self.timer = Some(Box::pin(R::sleep(target.saturating_duration_since(now))));
      self.timer_deadline = Some(target);
    }
  }
}

impl<I: NodeId, R: Runtime> Future for QuicDriver<I, R> {
  type Output = ();

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
    let this = self.get_mut();
    let now = Instant::now();
    let mut progress = false;
    let mut more = false;

    // Drain queued commands (parks the waker for the next push).
    for cmd in this.shared.drain_commands(cx.waker()) {
      this.dispatch(cmd, now);
      progress = true;
    }

    // Shutdown: best-effort leave, flush once, fail any parked waiters, stop.
    if this.shared.is_shutdown() {
      // Ignoring Err: best-effort leave during shutdown.
      let _ = this.endpoint.leave(Instant::now());
      this.drain_surfaces(cx);
      // Close the command queue and fail any still-queued commands, so a handle
      // that raced the shutdown gets a reply instead of hanging.
      for cmd in this.shared.close_and_drain() {
        match cmd {
          // Ignoring Err: the caller dropped its reply receiver.
          Command::Join(JoinCmd { reply, .. }) => {
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::Leave(LeaveCmd { reply }) => {
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::Shutdown(ShutdownCmd { reply }) => {
            // A straggler `Shutdown` racing the first one: park it too, so it is
            // acked after the socket drop like every other caller. Never ack
            // inline here — the socket is still bound, so an inline ack would let
            // that caller rebind into a still-open port.
            this.shutdown_reply.push(reply);
          }
          // Ignoring Err: the caller dropped its reply receiver.
          Command::Ping(PingCmd { reply, .. }) => {
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::SendUser(SendUserCmd { reply, .. }) => {
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::SendReliable(SendReliableCmd { reply, .. }) => {
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::SetCompressionOptions(SetCompressionOptionsCmd { reply, .. }) => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::SetChecksumOptions(SetChecksumOptionsCmd { reply, .. }) => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::SetEncryptionOptions(SetEncryptionOptionsCmd { reply, .. }) => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::UpdateNodeMetadata(UpdateNodeMetadataCmd { reply, .. }) => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::QueueUserBroadcast(QueueUserBroadcastCmd { reply, .. }) => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::SetLocalState(SetLocalStateCmd { reply, .. }) => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::SetAckPayload(SetAckPayloadCmd { reply, .. }) => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.send(Err(Error::Shutdown));
          }
        }
      }
      for (_, pj) in this.pending_joins.drain() {
        // Ignoring Err: the join caller dropped its reply receiver.
        let _ = pj.reply.send(Err(Error::Shutdown));
      }
      if let Some(pl) = this.pending_leave.take() {
        for replier in pl.repliers {
          // Ignoring Err: the leave caller dropped its reply receiver.
          let _ = replier.send(Err(Error::Shutdown));
        }
      }
      for pp in this.pending_pings.drain(..) {
        // Ignoring Err: the ping caller dropped its reply receiver.
        let _ = pp.reply.send(Err(Error::Shutdown));
      }
      for ps in this.pending_user_sends.drain(..) {
        // Ignoring Err: the send_reliable caller dropped its reply receiver.
        let _ = ps.reply.send(Err(Error::Shutdown));
      }
      // Release the bound port BEFORE acking the shutdown caller. Drop the UDP
      // socket, closing its FD synchronously (the agnostic `UdpSocket` has no
      // async `close`, so the local going out of scope here closes the FD). Only
      // after the port is free does the stashed reply fire, so a caller resuming
      // from `shutdown().await` can immediately rebind on the same address
      // without racing a still-open socket.
      drop(this.socket.take());
      for reply in this.shutdown_reply.drain(..) {
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(Ok(()));
      }
      // The port is now free; release any late `shutdown()` caller that found the
      // command queue already closed and parked on the completion latch.
      this.shared.mark_shutdown_complete();
      return Poll::Ready(());
    }

    // Receive gossip (bounded; a full batch means more may be waiting). The
    // socket is always `Some` here — the shutdown branch above (which takes it)
    // returned `Poll::Ready` before reaching this point.
    let mut recv_n = 0;
    while recv_n < this.recv_batch {
      let Some(socket) = this.socket.as_ref() else {
        break;
      };
      match socket.poll_recv_from(cx, &mut this.recv_buf) {
        Poll::Ready(Ok((n, src))) => {
          // Drop a UDP packet from a CIDR-blocked source before the QUIC machine
          // sees it: a blocked peer completes no handshake and forms no
          // connection (the advertised-address filter is the composed alive
          // delegate). The recv still counts toward the batch.
          if !cidr_blocks(&this.cidr_policy, src.ip()) {
            this.endpoint.handle_udp(src, &this.recv_buf[..n], now);
          }
          recv_n += 1;
        }
        // Ignoring Err: a transient recv error is non-fatal; re-armed next poll.
        Poll::Ready(Err(_)) => break,
        Poll::Pending => break,
      }
    }
    if recv_n > 0 {
      progress = true;
    }
    // A saturated batch (recv_n == recv_batch) means more datagrams may still be
    // queued in the socket; self-wake so the next poll drains the rest.
    if recv_n == this.recv_batch {
      more = true;
    }

    // Drain machine surfaces (bounded per surface).
    let (drained, drain_more) = this.drain_surfaces(cx);
    progress |= drained;
    more |= drain_more;

    // Timer: advance membership time on schedule. A past-due deadline fires
    // `handle_timeout` now; otherwise (re)arm and poll the sleep. This runs every
    // poll regardless of the receive batch: `drain_surfaces` above decoded the
    // inbound ingress to empty (a datagram-carried probe Ack is already applied),
    // and the probe FSM anchors success on an absolute `failure_deadline`, so an
    // Ack decoded slightly later still rescues the probe regardless of
    // handle_ack-vs-handle_timeout ordering — no recv-saturation gate is needed.
    let target = match this.endpoint.poll_timeout() {
      Some(d) => d.min(now + this.idle_wake),
      None => now + this.idle_wake,
    };
    if target <= now {
      this.endpoint.handle_timeout(now);
      progress = true;
      more = true;
    } else {
      this.arm_timer(target, now);
      if let Some(timer) = this.timer.as_mut()
        && timer.as_mut().poll(cx).is_ready()
      {
        this.endpoint.handle_timeout(Instant::now());
        this.timer = None;
        this.timer_deadline = None;
        progress = true;
        more = true;
      }
    }

    // Republish the snapshot only when membership/health actually changed (a
    // rebuild clones every NodeState), not on every productive poll.
    let snap_v = this.endpoint.endpoint_ref().snapshot_version();
    if progress && snap_v != this.last_snapshot_version {
      this.last_snapshot_version = snap_v;
      this
        .shared
        .publish(snapshot_of(this.endpoint.endpoint_ref()));
    }
    let metrics = this.endpoint.metrics();
    if metrics != this.last_metrics {
      this.last_metrics = metrics;
      this.shared.publish_metrics(metrics);
    }

    // Yield to other tasks, but re-poll promptly while work remains.
    if more {
      cx.waker().wake_by_ref();
    }
    Poll::Pending
  }
}

#[cfg(all(test, feature = "quic-rustls-ring"))]
mod tests {
  use std::{
    sync::atomic::AtomicBool,
    task::{Context, Wake, Waker},
    thread,
  };

  use agnostic::{net::Net, tokio::TokioRuntime};
  use memberlist_proto::{
    ChecksumOptions, CompressionOptions, EncryptionOptions, Node, QuicOptions, UnreliableTransport,
    config::EndpointOptions,
    endpoint::Endpoint,
    event::{Reliability, UserPacket},
    typed::{NodeState, State},
  };
  use quinn_proto::{ClientConfig, EndpointConfig, ServerConfig, TransportConfig};
  use rustls::RootCertStore;
  use rustls_pki_types::{CertificateDer, PrivateKeyDer};
  use smol_str::SmolStr;

  use super::*;
  use crate::command::{
    JoinCmd, LeaveCmd, PingCmd, SendReliableCmd, SendUserCmd, SetChecksumOptionsCmd,
    SetCompressionOptionsCmd, SetEncryptionOptionsCmd, ShutdownCmd,
  };

  type TokioNet = <TokioRuntime as Runtime>::Net;

  const ALPN: &[u8] = b"memberlist-quic-cov";

  /// A self-signed-and-self-trusted `QuicOptions` (QUIC-datagram unreliable
  /// transport) for the in-process driver. Mirrors the smoke-test fixtures; these
  /// driver tests never actually establish a connection, so a single self-trusted
  /// identity suffices.
  fn self_trusted_quic() -> QuicOptions {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("rcgen");
    let cert = CertificateDer::from(ck.cert.der().to_vec());
    let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone()).expect("root");

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut rustls_server = rustls::ServerConfig::builder_with_provider(provider.clone())
      .with_protocol_versions(&[&rustls::version::TLS13])
      .expect("TLS 1.3")
      .with_no_client_auth()
      .with_single_cert(vec![cert], key)
      .expect("server cert");
    rustls_server.alpn_protocols = vec![ALPN.to_vec()];
    let qsc = quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server))
      .expect("QuicServerConfig");
    let server_cfg = ServerConfig::with_crypto(Arc::new(qsc));

    let mut rustls_client = rustls::ClientConfig::builder_with_provider(provider)
      .with_protocol_versions(&[&rustls::version::TLS13])
      .expect("TLS 1.3")
      .with_root_certificates(roots)
      .with_no_client_auth();
    rustls_client.alpn_protocols = vec![ALPN.to_vec()];
    let qcc = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_client))
      .expect("QuicClientConfig");
    let client_cfg = ClientConfig::new(Arc::new(qcc));

    let hmac = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &[0x5au8; 32]);
    let endpoint_cfg = EndpointConfig::new(Arc::new(hmac));
    let transport = TransportConfig::default();
    QuicOptions::new(
      endpoint_cfg,
      server_cfg,
      client_cfg,
      transport,
      "localhost",
      UnreliableTransport::Datagram,
    )
  }

  /// A harmless waker (safe `std::task::Wake`, no `unsafe`).
  fn flag_waker() -> Waker {
    struct W(Arc<AtomicBool>);
    impl Wake for W {
      fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
      }
      fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
      }
    }
    Waker::from(Arc::new(W(Arc::new(AtomicBool::new(false)))))
  }

  /// An app-data `UserPacket` of `len` payload bytes (drives the obs byte
  /// backstop; a no-op for `account_event`).
  fn user_packet(len: usize) -> Event<SmolStr, SocketAddr> {
    Event::UserPacket(UserPacket::new(
      "127.0.0.1:2".parse::<SocketAddr>().unwrap(),
      Bytes::from(vec![0xABu8; len]),
      Reliability::Reliable,
    ))
  }

  /// A control event carrying no app-data and (with no parked state) a no-op for
  /// `account_event`.
  fn control_event() -> Event<SmolStr, SocketAddr> {
    Event::NodeJoined(Arc::new(NodeState::new(
      SmolStr::new("ctl"),
      "127.0.0.1:3".parse::<SocketAddr>().unwrap(),
      State::Alive,
    )))
  }

  /// Builds a real `QuicDriver` over a bound gossip socket with a caller-supplied
  /// observation channel, so the obs-backstop and shutdown branches can be driven
  /// directly. Returns the driver, the obs receiver (drop it for `Disconnected`),
  /// the shared state, and the payload-byte counter.
  async fn build_driver(
    obs_cap: usize,
    obs_budget: Option<u64>,
  ) -> (
    QuicDriver<SmolStr, TokioRuntime>,
    flume::Receiver<Event<SmolStr, SocketAddr>>,
    Arc<Shared<SmolStr>>,
    Arc<AtomicU64>,
  ) {
    let socket = <TokioNet as Net>::UdpSocket::bind("127.0.0.1:0")
      .await
      .expect("bind gossip socket");
    let ep = Endpoint::new(EndpointOptions::new(
      SmolStr::new("qdrv"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    ));
    let mut endpoint = QuicEndpoint::new(ep, self_trusted_quic());
    endpoint.start_scheduling(Instant::now());
    let shared = Arc::new(Shared::new(snapshot_of(endpoint.endpoint_ref())));
    let obs_payload_bytes = Arc::new(AtomicU64::new(0));
    let (obs_tx, obs_rx) = flume::bounded(obs_cap);
    let driver = QuicDriver::<SmolStr, TokioRuntime>::new(
      endpoint,
      socket,
      shared.clone(),
      8,
      8,
      obs_tx,
      obs_payload_bytes.clone(),
      obs_budget,
      None,
      #[cfg(feature = "cidr")]
      None,
      #[cfg(not(feature = "cidr"))]
      (),
    );
    (driver, obs_rx, shared, obs_payload_bytes)
  }

  /// Drives one `Future::poll` with a harmless waker.
  fn poll_once(driver: &mut QuicDriver<SmolStr, TokioRuntime>) -> Poll<()> {
    let waker = flag_waker();
    let mut cx = Context::from_waker(&waker);
    Pin::new(driver).poll(&mut cx)
  }

  /// `send_observation`'s byte backstop drops (and counts) a payload event that
  /// would push the queued payload bytes over budget; it is never retained.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn obs_byte_backstop_drops_oversized_payload() {
    let (mut driver, _obs_rx, shared, bytes) = build_driver(16, Some(4)).await;
    driver.send_observation(user_packet(8));
    assert_eq!(
      shared.observation_dropped(),
      1,
      "over-budget payload dropped + counted"
    );
    assert_eq!(
      bytes.load(Ordering::Relaxed),
      0,
      "a dropped payload reserves no bytes"
    );
    assert!(
      driver.obs_overflow.is_empty(),
      "a byte-backstop drop retains nothing"
    );
  }

  /// A FULL obs channel RETAINS application data (still byte-reserved) for retry.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn obs_full_channel_retains_app_data() {
    let (mut driver, _obs_rx, shared, bytes) = build_driver(1, Some(1 << 20)).await;
    driver.send_observation(user_packet(4)); // fills the capacity-1 channel
    assert!(
      driver.obs_overflow.is_empty(),
      "first event went to the channel"
    );
    driver.send_observation(user_packet(7)); // channel full → retained
    assert_eq!(
      driver.obs_overflow.len(),
      1,
      "app-data retained on a full channel"
    );
    assert_eq!(
      shared.observation_dropped(),
      0,
      "a retained event is not a drop"
    );
    assert_eq!(
      bytes.load(Ordering::Relaxed),
      4 + 7,
      "both payloads stay byte-reserved"
    );
  }

  /// A FULL obs channel DROPS (and counts) a recoverable control event.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn obs_full_channel_drops_recoverable_control() {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(1, Some(1 << 20)).await;
    driver.send_observation(control_event()); // fills the channel
    driver.send_observation(control_event()); // full → dropped + counted
    assert!(
      driver.obs_overflow.is_empty(),
      "a control event is never retained"
    );
    assert_eq!(
      shared.observation_dropped(),
      1,
      "the dropped control event is counted"
    );
  }

  /// With the obs task gone, `send_observation` rolls back its reservation.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn obs_disconnected_rolls_back_reservation() {
    let (mut driver, obs_rx, shared, bytes) = build_driver(16, Some(1 << 20)).await;
    drop(obs_rx); // the only receiver is gone → the driver's sender sees Disconnected
    driver.send_observation(user_packet(9));
    assert_eq!(
      bytes.load(Ordering::Relaxed),
      0,
      "Disconnected rolls back the reservation"
    );
    assert!(
      driver.obs_overflow.is_empty(),
      "Disconnected retains nothing"
    );
    assert_eq!(
      shared.observation_dropped(),
      0,
      "Disconnected is not a recoverable drop"
    );
  }

  /// `flush_obs_overflow` stops at the first `Full`, re-pushing to the front.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn flush_overflow_stops_and_repushes_on_full() {
    let (mut driver, _obs_rx, _shared, _bytes) = build_driver(1, Some(1 << 20)).await;
    driver
      .obs_tx
      .try_send(control_event())
      .expect("seed the channel full");
    driver.obs_overflow.push_back(control_event());
    driver.obs_overflow.push_back(control_event());
    driver.flush_obs_overflow();
    assert_eq!(
      driver.obs_overflow.len(),
      2,
      "flush stops at the first Full and re-pushes"
    );
  }

  /// `flush_obs_overflow` with the obs task gone reclaims retained payload bytes.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn flush_overflow_disconnected_reclaims_bytes() {
    let (mut driver, obs_rx, _shared, bytes) = build_driver(16, Some(1 << 20)).await;
    bytes.store(6, Ordering::Relaxed);
    driver.obs_overflow.push_back(user_packet(6));
    drop(obs_rx); // the only receiver is gone → the flush sees Disconnected
    driver.flush_obs_overflow();
    assert!(
      driver.obs_overflow.is_empty(),
      "a Disconnected flush drains the overflow"
    );
    assert_eq!(
      bytes.load(Ordering::Relaxed),
      0,
      "a Disconnected flush reclaims the bytes"
    );
  }

  /// On shutdown, a parked `WaitForCompletion` join is failed with `Shutdown`
  /// (the `pending_joins.drain()` arm).
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn shutdown_fails_parked_join() {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
    let (tx, rx) = futures_channel::oneshot::channel::<Result<usize, Error>>();
    shared.push_command(Command::Join(JoinCmd {
      addrs: vec!["127.0.0.1:9".parse::<SocketAddr>().unwrap()],
      wait: true,
      reply: tx,
    }));
    shared.begin_shutdown();
    assert!(poll_once(&mut driver).is_ready());
    assert!(
      matches!(rx.await, Ok(Err(Error::Shutdown))),
      "a parked wait-join is failed with Shutdown on driver exit"
    );
  }

  /// On shutdown, a parked application-ping is failed with `Shutdown` (the
  /// `pending_pings.drain()` arm).
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn shutdown_fails_parked_ping() {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
    let (tx, rx) = futures_channel::oneshot::channel::<Result<Duration, Error>>();
    let node = Node::new(
      SmolStr::new("peer"),
      "127.0.0.1:9".parse::<SocketAddr>().unwrap(),
    );
    shared.push_command(Command::Ping(PingCmd { node, reply: tx }));
    shared.begin_shutdown();
    assert!(poll_once(&mut driver).is_ready());
    assert!(
      matches!(rx.await, Ok(Err(Error::Shutdown))),
      "a parked ping is failed with Shutdown on driver exit"
    );
  }

  /// On shutdown, a parked reliable directed send is failed with `Shutdown` (the
  /// `pending_user_sends.drain()` arm).
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn shutdown_fails_parked_reliable_send() {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
    let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    shared.push_command(Command::SendReliable(SendReliableCmd {
      to: "127.0.0.1:9".parse::<SocketAddr>().unwrap(),
      payloads: vec![Bytes::from_static(b"reliable")],
      reply: tx,
    }));
    shared.begin_shutdown();
    assert!(poll_once(&mut driver).is_ready());
    assert!(
      matches!(rx.await, Ok(Err(Error::Shutdown))),
      "a parked reliable send is failed with Shutdown on driver exit"
    );
  }

  /// On shutdown, an in-flight graceful leave's waiter(s) resolve with `Shutdown`
  /// (the `pending_leave.take()` arm). The endpoint is first driven to `Left` so
  /// the shutdown's own no-op `leave()` emits no `LeftCluster` that would resolve
  /// the seeded waiter early; it then survives the drain to the shutdown arm.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn shutdown_fails_parked_leave() {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
    let (warm_tx, _warm_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    shared.push_command(Command::Leave(LeaveCmd { reply: warm_tx }));
    assert!(
      poll_once(&mut driver).is_pending(),
      "warm-up poll keeps running"
    );
    assert!(
      driver.pending_leave.is_none(),
      "the no-peer leave completed in the warm-up"
    );
    assert!(
      !driver.endpoint.is_running(),
      "the endpoint is Left; shutdown leave() is a no-op"
    );

    let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    driver.pending_leave = Some(PendingLeave { repliers: vec![tx] });
    shared.begin_shutdown();
    assert!(poll_once(&mut driver).is_ready());
    assert!(
      matches!(rx.await, Ok(Err(Error::Shutdown))),
      "a parked leave waiter is failed with Shutdown on driver exit"
    );
  }

  /// The shutdown branch's `close_and_drain` fails EVERY queued command variant
  /// with `Shutdown`. Every command type is queued while the driver still
  /// accepts pushes, then shutdown begins; the next poll runs `close_and_drain`,
  /// which takes the whole queue and replies `Shutdown` to each replier. The
  /// five distinguishable variants (join, user, compression, checksum,
  /// encryption) are checked.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn shutdown_close_and_drain_fails_every_queued_command() {
    use std::sync::Barrier;

    const MAX_ATTEMPTS: usize = 20000;
    // Each command variant must, in SOME attempt, be pushed into the narrow
    // window between the poll's top-of-poll `drain_commands` and its
    // `close_and_drain`, so that `close_and_drain` (not normal dispatch) fails
    // it with `Shutdown`. The window is racy, so accumulate per-variant rather
    // than demanding all five in one attempt: every variant lands in it within
    // the bound, and the loop breaks as soon as all five have been observed.
    // The push order is rotated each attempt so no variant is starved by always
    // racing from the same position.
    let mut seen_join = false;
    let mut seen_user = false;
    let mut seen_comp = false;
    let mut seen_chk = false;
    let mut seen_enc = false;
    for attempt in 0..MAX_ATTEMPTS {
      let (mut driver, _obs_rx, shared, _bytes) = build_driver(64, Some(1 << 20)).await;
      shared.begin_shutdown();

      let (join_tx, join_rx) = futures_channel::oneshot::channel::<Result<usize, Error>>();
      let (user_tx, user_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
      let (comp_tx, comp_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
      let (chk_tx, chk_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
      let (enc_tx, enc_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
      let (leave_tx, _leave_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
      let (shutdown_tx, _shutdown_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
      let (ping_tx, _ping_rx) = futures_channel::oneshot::channel::<Result<Duration, Error>>();
      let (rel_tx, _rel_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();

      let to = "127.0.0.1:9".parse::<SocketAddr>().unwrap();
      let node = Node::new(SmolStr::new("peer"), to);
      let mut cmds: Vec<Command<SmolStr>> = vec![
        Command::Join(JoinCmd {
          addrs: vec![to],
          wait: false,
          reply: join_tx,
        }),
        Command::SendUser(SendUserCmd {
          to,
          payloads: vec![Bytes::from_static(b"u")],
          reply: user_tx,
        }),
        Command::SetCompressionOptions(SetCompressionOptionsCmd {
          opts: CompressionOptions::new(),
          reply: comp_tx,
        }),
        Command::SetChecksumOptions(SetChecksumOptionsCmd {
          opts: ChecksumOptions::new(),
          reply: chk_tx,
        }),
        Command::SetEncryptionOptions(SetEncryptionOptionsCmd {
          opts: EncryptionOptions::new(),
          reply: enc_tx,
        }),
        Command::Leave(LeaveCmd { reply: leave_tx }),
        Command::Shutdown(ShutdownCmd { reply: shutdown_tx }),
        Command::Ping(PingCmd {
          node,
          reply: ping_tx,
        }),
        Command::SendReliable(SendReliableCmd {
          to,
          payloads: vec![Bytes::from_static(b"r")],
          reply: rel_tx,
        }),
      ];
      // Rotate the push order so each variant races from a different position
      // across attempts rather than always last (which would starve it).
      let rotation = attempt % cmds.len();
      cmds.rotate_left(rotation);

      let barrier = Arc::new(Barrier::new(2));
      let pusher_barrier = barrier.clone();
      let pusher_shared = shared.clone();
      let pusher = thread::spawn(move || {
        pusher_barrier.wait();
        for cmd in cmds {
          // Ignoring bool: a push rejected after the queue closed just means
          // this attempt missed the window for that command; the outer loop
          // retries and another attempt will catch it.
          let _ = pusher_shared.push_command(cmd);
        }
      });

      barrier.wait();
      assert!(
        poll_once(&mut driver).is_ready(),
        "a shutdown poll returns Ready"
      );
      pusher.join().expect("pusher thread joins");

      seen_join |= matches!(join_rx.await, Ok(Err(Error::Shutdown)));
      seen_user |= matches!(user_rx.await, Ok(Err(Error::Shutdown)));
      seen_comp |= matches!(comp_rx.await, Ok(Err(Error::Shutdown)));
      seen_chk |= matches!(chk_rx.await, Ok(Err(Error::Shutdown)));
      seen_enc |= matches!(enc_rx.await, Ok(Err(Error::Shutdown)));
      if seen_join && seen_user && seen_comp && seen_chk && seen_enc {
        break;
      }
    }
    assert!(
      seen_join && seen_user && seen_comp && seen_chk && seen_enc,
      "every queued command variant must reply Shutdown via close_and_drain within \
       {MAX_ATTEMPTS} attempts (join={seen_join} user={seen_user} comp={seen_comp} \
       chk={seen_chk} enc={seen_enc})"
    );
  }

  /// Two `Shutdown` commands queued before the SAME poll each get their own
  /// `Ok(())` ack. The shutdown reply is a `Vec`, so the first caller is parked
  /// alongside the second rather than overwritten — a single-slot reply would
  /// drop the first sender (its receiver would observe a `Canceled` oneshot)
  /// when the second `Shutdown` dispatched in the same drain.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn shutdown_acks_every_same_poll_caller() {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;

    let (first_tx, first_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    let (second_tx, second_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    // Both land in one top-of-poll drain, so both dispatch (and park their reply)
    // before the shutdown branch acks.
    shared.push_command(Command::Shutdown(ShutdownCmd { reply: first_tx }));
    shared.push_command(Command::Shutdown(ShutdownCmd { reply: second_tx }));

    assert!(
      poll_once(&mut driver).is_ready(),
      "a shutdown poll returns Ready"
    );
    assert!(
      matches!(first_rx.await, Ok(Ok(()))),
      "the first same-poll shutdown caller is acked Ok, not dropped"
    );
    assert!(
      matches!(second_rx.await, Ok(Ok(()))),
      "the second same-poll shutdown caller is acked Ok"
    );
  }

  /// A second `Shutdown` racing the poll while one is already parked is itself
  /// acked `Ok(())` — whether it is dispatched at the top of the poll or taken
  /// by `close_and_drain` mid-poll — and the already-parked first caller is
  /// STILL acked `Ok(())`. With a single-slot reply the second caller would
  /// overwrite the first's parked sender (canceling its oneshot) when both land
  /// in the same drain; the reply set holds every concurrent caller instead.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn shutdown_acks_concurrent_callers() {
    use std::sync::Barrier;

    const MAX_ATTEMPTS: usize = 20000;
    // The first `Shutdown` is queued before the poll, so it is always drained and
    // parked. A pusher thread races a second `Shutdown` into the poll: it lands
    // either in the same top-of-poll drain as the first or in the window before
    // `close_and_drain`. The first caller's ack must survive that race in EVERY
    // attempt; the second's `Ok(())` is recorded when its push was accepted, to
    // confirm the concurrent path is actually exercised within the bound.
    let mut saw_second_ok = false;
    for _ in 0..MAX_ATTEMPTS {
      let (mut driver, _obs_rx, shared, _bytes) = build_driver(64, Some(1 << 20)).await;
      shared.begin_shutdown();

      let (first_tx, first_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
      let (second_tx, second_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
      // Queue the first caller before polling so it is always parked.
      shared.push_command(Command::Shutdown(ShutdownCmd { reply: first_tx }));

      let barrier = Arc::new(Barrier::new(2));
      let pusher_barrier = barrier.clone();
      let pusher_shared = shared.clone();
      // The push returns false if the poll already closed the queue; in that case
      // the second caller never enters and this attempt simply does not exercise
      // the concurrent path. Report whether it was accepted so the assertion can
      // ignore the receiver of a never-queued caller.
      let pusher = thread::spawn(move || -> bool {
        pusher_barrier.wait();
        pusher_shared.push_command(Command::Shutdown(ShutdownCmd { reply: second_tx }))
      });

      barrier.wait();
      assert!(
        poll_once(&mut driver).is_ready(),
        "a shutdown poll returns Ready"
      );
      let second_queued = pusher.join().expect("pusher thread joins");

      // The first, always-parked caller must be acked Ok regardless of how the
      // second raced — a single-slot reply would drop it on a same-drain overwrite.
      assert!(
        matches!(first_rx.await, Ok(Ok(()))),
        "the already-parked shutdown caller is acked Ok despite a concurrent shutdown"
      );
      // When the second push was accepted, its caller must also be acked Ok
      // (parked at dispatch or via close_and_drain), never left hanging.
      if second_queued {
        assert!(
          matches!(second_rx.await, Ok(Ok(()))),
          "an accepted concurrent shutdown caller is also acked Ok"
        );
        saw_second_ok = true;
      }
    }
    assert!(
      saw_second_ok,
      "a concurrent second shutdown must be accepted and acked Ok in some attempt within \
       {MAX_ATTEMPTS}"
    );
  }
}
