//! Sans-I/O state machine for the [`memberlist`](https://crates.io/crates/memberlist) gossip
//! protocol. Modeled on [`quinn-proto`](https://docs.rs/quinn-proto): a pure state machine
//! that takes events as inputs and emits actions as outputs, never owning I/O or timers.
//!
//! See `docs/superpowers/specs/2026-05-10-memberlist-machine-design.md` for the design.
//!
//! This crate is `Send` but not `Sync` per type. Drivers choose how to share access
//! (mutex, dedicated thread, actor model, etc.).
//!
//! # The driver contract
//!
//! This crate is **only the protocol logic**. Sockets, the clock, TLS,
//! retransmit timers, task scheduling, and event delivery ordering are the
//! *driver's* job. The two responsibilities do not mix: the state machine
//! is correct on its own terms, and it will **not** contain machinery to
//! paper over a driver-layer problem. Read this section before writing a
//! driver.
//!
//! ## The two state machines
//!
//! * [`endpoint::Endpoint`] — one per local node. Owns membership
//!   (alive/suspect/dead/refute/merge), the SWIM probe FSM, Lifeguard
//!   awareness, suspicion timers, and the gossip broadcast queue.
//! * [`stream::Stream`] — one per *reliable exchange* (push/pull, reliable
//!   ping, reliable user message). Minted by
//!   [`Endpoint::dial_succeeded`](endpoint::Endpoint::dial_succeeded)
//!   (outbound) or [`Endpoint::accept_stream`](endpoint::Endpoint::accept_stream)
//!   (inbound).
//!
//! The driver **bridges** them: drain a `Stream`'s
//! [`poll_endpoint_event`](stream::Stream::poll_endpoint_event) and feed
//! each into
//! [`Endpoint::handle_stream_event`](endpoint::Endpoint::handle_stream_event).
//!
//! ## The driver loop
//!
//! Per wake-up the driver, holding whatever lock it chose:
//!
//! 1. feeds inbound datagrams via
//!    [`handle_packet`](endpoint::Endpoint::handle_packet) (or the typed
//!    `handle_*` methods) with the **observed transport source address**
//!    and the current `now`;
//! 2. pumps each live `Stream`: `handle_data` for received bytes (or an
//!    **empty slice for EOF** — see below), drain `poll_transmit(now, ..)`
//!    to the socket, route `poll_endpoint_event` into the `Endpoint`,
//!    surface `poll_event`, and reap the stream once
//!    [`is_done`](stream::Stream::is_done) or
//!    [`is_failed`](stream::Stream::is_failed);
//! 3. fires [`Endpoint::handle_timeout(now)`](endpoint::Endpoint::handle_timeout)
//!    (and each Stream's `handle_timeout`);
//! 4. drains [`poll_transmit`](endpoint::Endpoint::poll_transmit) and
//!    [`poll_event`](endpoint::Endpoint::poll_event) **until `None`**;
//! 5. sleeps until [`poll_timeout`](endpoint::Endpoint::poll_timeout)
//!    (min'd with each Stream's `poll_timeout`), then repeats.
//!
//! ## Time contract
//!
//! Every entry point that can advance state takes `now: Instant`. The
//! driver **MUST** pass a *non-decreasing* `now` across calls. Internal
//! deadlines are anchored absolutely at the instant they are created
//! (e.g. a probe's failure deadline is `sent + interval`, snapshotted
//! once), never recomputed from a later injected `now` — so a late timer
//! callback or packet-vs-timer reordering cannot move them. Over-firing
//! `handle_timeout` (calling it before any deadline elapsed) is always
//! safe and is a no-op.
//!
//! ## Correctness vs. quality (read this)
//!
//! The state machine is **correct under ANY input ordering**. A late or
//! out-of-order event never corrupts state: a deadline is authoritative
//! at the point of action (an ack/transmit/read whose `now >= deadline`
//! is rejected, not retroactively undone), and any residual disagreement
//! converges through the protocol's own self-healing (e.g. a node briefly
//! suspected by a lost race refutes with a higher incarnation — exactly
//! as upstream memberlist behaves). The driver therefore does **not** need
//! to pre-filter stale input for correctness.
//!
//! What *does* depend on the driver is **quality**: failure-detection
//! latency and how often a healthy node is transiently suspected. These
//! improve the more promptly and in-causal-order the driver delivers
//! input. In particular, routing a reliable `Stream`'s endpoint events
//! into the `Endpoint` *before* firing the `Endpoint`'s cumulative-probe
//! `handle_timeout` narrows the transient-suspect window to upstream
//! parity. That is a driver-side optimization, **not** a correctness
//! requirement the machine relies on — the machine never compensates for
//! a driver that delivers late; it just self-heals, and the driver owns
//! making delivery prompt.
//!
//! ## Stream specifics
//!
//! * **EOF is an empty slice.** The driver signals peer-close / clean
//!   shutdown by calling
//!   [`Stream::handle_data(&[], now)`](stream::Stream::handle_data). A
//!   non-terminal stream treats that as `PeerClosed`.
//! * **Draining `poll_transmit` until `None` is the whole write
//!   contract.** Handing over the last byte is what advances the write
//!   phase; there is no separate "write complete" call.
//! * A `Stream` is *minted by* the `Endpoint` but driven *independently*;
//!   it carries the exchange deadline and is the authority on it.

#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![allow(clippy::type_complexity, unexpected_cfgs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

pub mod ack;
pub mod awareness;
pub mod broadcast;
pub mod config;
pub mod delegate;
pub mod endpoint;
pub mod error;
pub mod event;
pub mod members;
pub(crate) mod probe;
pub mod stream;
pub mod suspicion;
pub(crate) mod wire;

pub use ack::{AckEntry, AckKind, AckRegistry, AckResolution, ForwardAck};
pub use awareness::Awareness;
pub use broadcast::{Broadcast, BroadcastQueue, MemberlistBroadcast};
pub use config::EndpointConfig;
pub use delegate::{AliveDelegate, MergeDelegate};
pub use endpoint::{Endpoint, Lifecycle, META_MAX_SIZE};
pub use error::{Error, StreamError};
pub use event::{
  CompoundTransmit, EndpointEvent, Event, PacketTransmit, PushPullKind, Reliability, StreamCommand,
  StreamEvent, StreamId, Transmit,
};
pub use members::{LocalNodeState, Member, Members};
pub use stream::{PushPullSnapshot, Stream};
pub use suspicion::{Confirmation, Suspicion};
