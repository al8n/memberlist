# Workspace layout — live crates vs. frozen backups

As of the Phase 8 (driver consolidation) + Phase 9 (buffa wire codec)
refactor, the workspace is organized like `quinn`'s: a high-level async
umbrella crate built directly on a Sans-I/O state machine.

## Live crates (the current stack)

| Crate | Role |
|---|---|
| `memberlist` | High-level async driver crate (analog of `quinn`). Public API: [`TcpMemberlist`] and [`QuicMemberlist`], built on `memberlist-machine` + `memberlist-wire`, with Sans-I/O rustls TLS and a quinn-proto QUIC driver. **New code should use this crate.** |
| `memberlist-machine` | Sans-I/O membership state machine (analog of `quinn-proto`). No I/O; driven by the umbrella's async driver. |
| `memberlist-wire` | buffa-generated wire codec + framing. The successor to `memberlist-proto`; what `memberlist-machine` speaks since Phase 9. |
| `memberlist-simulation` | Deterministic single-threaded simulation harness driving `memberlist-machine` without sockets or a runtime. |
| `memberlist-core` | **Still a live dependency** of the `memberlist` umbrella. The legacy `Memberlist<T, D>` and the `Transport` trait are re-exported from the umbrella for backwards compatibility, and the umbrella's crypto / compression / checksum / serde / metrics features forward to it. Not frozen — it is part of the active build. |

## Frozen backups (reference only — NOT in the active build)

These crates are **frozen** as of the refactor. They are excluded from
the workspace (`exclude` in the root `Cargo.toml`), are kept on disk only
as reference, and are **not expected to compile** against the
post-Phase-9 `memberlist-machine`. They will not be deleted.

| Crate | Status | Superseded by |
|---|---|---|
| `memberlist-net` | Frozen Phase-7 driver reference (`driver.rs`, `codec.rs`, `memberlist.rs` — the first machine-based TCP/UDP driver experiment). Its driver code targets the pre-Phase-9 machine API and no longer compiles. | `memberlist::TcpMemberlist` + `memberlist::driver_net` (re-implemented directly on agnostic sockets in the umbrella). |
| `memberlist-quic` | Frozen Phase-7 Task 7.0 QUIC scaffolding. | `memberlist::QuicMemberlist` + `memberlist::driver_quic` (quinn-proto driver in the umbrella). |
| `memberlist-proto` | Frozen hand-rolled wire codec. Still builds standalone (self-contained; used by the `fuzz` crate and, transitively, by the live `memberlist-core`), but `memberlist-machine` no longer depends on it. | `memberlist-wire` (buffa-generated codegen + framing). |

Phase 9 was a clean cut-over: the `memberlist-proto` and `memberlist-wire`
wire formats are **not** interoperable (the inner message body differs —
`memberlist-proto` uses a custom high-3-bits wire-type scheme,
`memberlist-wire` uses standard protobuf via buffa). Mixed-version
clusters across the cut-over are unsupported.

## Why this file (and not per-crate `BACKUP.md` / `lib.rs` edits)

The original Phase 8 plan called for a `BACKUP.md` inside each frozen
crate plus a doc-comment banner prepended to each frozen `lib.rs`. That
would modify files inside the frozen crate directories. The frozen crates
are kept byte-for-byte unmodified (their working trees must stay clean),
so the backup status is documented here at the workspace root instead.
