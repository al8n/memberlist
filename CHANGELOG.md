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
