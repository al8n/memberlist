# Releases

## Unreleased — Sans-I/O rewrite

A ground-up rewrite of memberlist as a synchronous, single-owner Sans-I/O state
machine, with the async runtime concerns moved into thin per-runtime drivers.

- **`memberlist-proto`** — the deterministic, no-locks SWIM state machine
  (membership, failure detection, gossip, push/pull, reliable streams over
  TCP/TLS/QUIC, the codec, and the optional compression / checksum / encryption
  transforms). `no_std + alloc` capable.
- **`memberlist`** — the umbrella facade re-exporting the machine and the async
  drivers behind runtime features.
- **Drivers** — `memberlist-reactor` (tokio / smol via `agnostic`),
  `memberlist-compio` (io_uring), and the embedded line (`memberlist-embedded`,
  `memberlist-smoltcp`, `memberlist-embassy`).

This release line forms new↔new clusters only; it does not interoperate on the
wire with the legacy `0.x` hand-rolled codec below.

### API notes

- `Event` gains a `DialAborted` variant (with a `DialAborted<A>` payload and a
  `DialAbortReason` enum). It completes the stream backend's dial-lifecycle
  totality: every `start_*`-returned `StreamId` now receives exactly one
  machine-emitted terminal — a synchronous `Err`, a `DialAborted` (retired
  before its `Connect` was drained, including a graceful leave that cancels a
  still-queued `Connect`), or an `eid`-keyed `ExchangeCompleted` once the
  `Connect` has been drained. A graceful `leave` now also promptly terminalizes
  every cancelled outbound reliable exchange (`ExchangeCompleted(Failed)` for a
  drained `Connect`, `DialAborted(Leaving)` for a still-queued one) so a parked
  reliable-send / join waiter resolves at leave rather than hanging until
  shutdown. `Event` is deliberately exhaustive and gains variants in `0.x`
  minor releases; match it exhaustively.
- `StreamEndpoint` gains `feed_advances_membership_time` /
  `set_feed_advances_membership_time`. By default (`true`) the reliable-plane
  feeds (`handle_transport_data`, `handle_transport_error`,
  `handle_dial_failed`) run the full coordinator tick, membership sweep
  included — today's behavior, unchanged. A driver that feeds several
  connections per wake can clear it so `Endpoint::handle_timeout` runs only
  from the coordinator's own `handle_timeout`, making the driver's explicit
  tick the single membership sweep of a wake; evidence-driven transitions
  (a reliable ping ack against its probe's deadline, a merged remote
  `Left` / `Dead` / `Suspect`) still apply at every feed instant. The embedded
  engine opts in, so its pump applies all of a pump's gossip and reliable
  input before one sweep; the async drivers keep the default.

### Behavior changes

- IPv4-mapped IPv6 advertise addresses (`::ffff:a.b.c.d`) are now canonicalized
  to their IPv4 form before the socket bind, so a node has a single identity
  across both address families. Previously such a configuration split the node
  into two membership entries (the same host keyed under both spellings) and let
  a `CidrPolicy` written in one family silently miss the other. `CidrPolicy`
  matching is likewise canonical-only: a checked IP is normalized with
  `to_canonical()` before every containment test, so a policy net must be written
  in its address's canonical family (a net inside the mapped range `::ffff:0:0/96`
  never matches). A deployment that advertised a mapped address now announces its
  IPv4 form — restart / rejoin such a node into this version rather than rolling
  it, as its published identity changes to the canonical spelling.

## 0.6.0

### Features

- Add `send_many` to let users send multiple packets through unreliable connection.
- Add `send_many_reliable` to let users send multiple packets through reliable connection.
- Redesign `Transport` trait, making it easier to implement for users.
- Rewriting encoding/decoding to support forward and backward compitibility.
- Support `zstd`, `brotli`, `lz4`, and `snappy` for compressing.
- Support `crc32`, `xxhash64`, `xxhash32`, `xxhash3`, `murmur3` for checksuming.
- Unify returned error, all exported APIs return `Error` on `Result::Err`.

### Example

- Add [`toydb`](./examples/toydb/) Example

### Breakage

- Remove `native-tls` supports
- Remove `s2n-quic` supports
- Remove `Wire` trait to simplify `Transport` trait
- Remove `JoinError`, add an new `Error::Multiple` variant

### Testing

- Add fuzzy testing for encoding/decoding
