# Workspace layout — current Sans-I/O stack vs. legacy crates

The workspace is organized like `quinn`'s: a Sans-I/O protocol crate
(`memberlist-proto`, the analog of `quinn-proto`) plus separate driver crates
that own the I/O. The previous high-level stack — the `memberlist` umbrella,
`memberlist-core`, and the hand-rolled wire/transport crates — is retained
under `legacy/` for reference.

## Current stack (workspace root)

| Crate | Role |
|---|---|
| `memberlist-proto` | Sans-I/O wire codec, framing, and SWIM membership state machine (analog of `quinn-proto`). Owns no sockets, clock, or timers — drivers feed it events and drain its actions. The buffa-generated wire message types and the hand-rolled outer framing live here alongside the state machine. |
| `memberlist-compio` | Completion-based async driver on the `compio` runtime (TCP, TLS, and QUIC over `memberlist-proto`). |
| `memberlist-reactor` | Reactor-I/O async driver, runtime-agnostic over `tokio` / `smol` (via `agnostic`). |
| `memberlist-smoltcp` | Executor-free `no_std` driver over the `smoltcp` TCP/IP stack, for bare-metal / embedded targets (a caller-owned poll loop, no runtime). |
| `memberlist-simulation` | Deterministic single-threaded conformance harness driving `memberlist-proto` without sockets or a runtime. |

## Legacy crates (`legacy/`)

The pre-Sans-I/O stack, kept for reference. `memberlist-core`, the
`memberlist` umbrella, and `memberlist-wire` still build (the umbrella is
transitional and is slated to be rebuilt directly on the current stack);
`memberlist-net` and `memberlist-quic` are excluded from the workspace build.

| Crate | Role |
|---|---|
| `legacy/memberlist` | The legacy high-level umbrella (`Memberlist<T, D>` re-exports). Transitional: it currently bridges `memberlist-core` and `memberlist-proto`. |
| `legacy/memberlist-core` | The legacy high-level `Memberlist<T, D>` and `Transport` trait. A live dependency of the legacy umbrella; its crypto / compression / checksum / serde / metrics features forward to `memberlist-wire`. |
| `legacy/memberlist-wire` | The legacy hand-rolled wire codec and framing (formerly `memberlist-proto`). Used by `memberlist-core` and the `fuzz` crate. |
| `legacy/memberlist-net` | Frozen legacy TCP/UDP transport. Excluded from the build. |
| `legacy/memberlist-quic` | Frozen legacy QUIC transport. Excluded from the build. |

The current `memberlist-proto` wire format (protobuf message bodies via buffa)
and the legacy `memberlist-wire` format (a custom high-3-bits wire-type scheme)
are **not** interoperable; clusters do not mix the two.
