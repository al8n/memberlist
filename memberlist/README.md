<div align="center">
<h1>memberlist</h1>
</div>
<div align="center">

Batteries-included, runtime-agnostic **SWIM** gossip membership and failure
detection for Rust — a Sans-I/O protocol core with pluggable async drivers. A port
of [HashiCorp's memberlist].

[<img alt="github" src="https://img.shields.io/badge/github-al8n/memberlist-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/memberlist/coverage.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/memberlist?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-memberlist-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/memberlist?style=for-the-badge&logo=rust" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/memberlist?color=critical&logo=rust&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=mozilla" height="22">

</div>

## Introduction

`memberlist` is the high-level entry point to the memberlist family. It wires the
Sans-I/O protocol core ([`memberlist-proto`]) to a ready-to-use async driver
([`memberlist-reactor`]) so you can manage cluster membership and detect node failures
in a few lines, with `tokio` out of the box.

memberlist manages cluster membership and member failure detection using a gossip-based
protocol — the foundation any distributed system needs. It is eventually consistent but
converges quickly, and tolerates partitions by routing around potentially-dead nodes.
This is a Rust port of [HashiCorp's memberlist], extended with a Sans-I/O architecture
and `no_std` / bare-metal support.

## Highlights

- **Sans-I/O core.** All protocol logic lives in [`memberlist-proto`] as pure state
  machines — no sockets, threads, or clocks — making it deterministic and exhaustively
  unit-tested. The drivers only shuttle bytes and time in and out.
- **Runtime-agnostic.** Drive it from `tokio`, `smol`, or `compio` (thread-per-core)
  with no change to protocol behavior.
- **`no_std` and bare-metal.** The core runs on `alloc`, and [`memberlist-smoltcp`] /
  [`memberlist-embassy`] bring full SWIM membership to embedded targets.
- **SWIM + Lifeguard.** A faithful port of HashiCorp's memberlist: suspicion / refutation,
  indirect probes, push/pull anti-entropy, and the Lifeguard awareness extensions that
  keep detection robust under CPU starvation and network loss.
- **Pluggable transports.** Plain TCP, TLS-over-TCP (`rustls`), or QUIC (`quinn-proto`)
  reliable planes — each with a UDP / datagram gossip plane carrying opt-in checksum,
  compression, and AEAD encryption.
- **Customizable.** Bring your own `Id`, `Address`, `AddressResolver`, and delegates
  (alive / conflict / merge / event / node / ping).
- **Observable, à la carte.** Opt into `tracing` — compiled out when unused.
- **Config-file & CLI friendly.** Every `*Options` type optionally derives `serde` and
  `clap`, so configuration loads from a file or maps straight onto CLI flags + env (std-only).

## The family

The crates split protocol logic from I/O, mirroring the `quinn` layering:

| Crate | Role |
|-------|------|
| [`memberlist`] | this crate — batteries-included facade (core + default `tokio` driver) |
| [`memberlist-proto`] | Sans-I/O protocol state machines (`no_std`-capable) |
| [`memberlist-reactor`] | runtime-agnostic async driver (`tokio` & `smol`) |
| [`memberlist-compio`] | `compio` (thread-per-core, io_uring / IOCP) async driver |
| [`memberlist-embedded`] | shared `no_std` driving core for the embedded drivers |
| [`memberlist-smoltcp`] | executor-free `no_std` driver over smoltcp (caller-poll) |
| [`memberlist-embassy`] | embassy-net async `no_std` driver, built on `memberlist-embedded` |

## Installation

```toml
[dependencies]
memberlist = "0.9" # tokio runtime + tcp transport by default
```

For `smol` instead of `tokio`:

```toml
[dependencies]
memberlist = { version = "0.9", default-features = false, features = ["smol", "tcp"] }
```

For the `compio` (completion-based, thread-per-core) runtime:

```toml
[dependencies]
memberlist = { version = "0.9", default-features = false, features = ["compio", "tcp"] }
```

For bare-metal (`no_std`) targets, enable `smoltcp` (the executor-free engine) or
`embassy` (the embassy-net async driver) — neither pulls in `std`:

```toml
[dependencies]
memberlist = { version = "0.9", default-features = false, features = ["embassy", "tcp"] }
```

The minimum supported Rust version (MSRV) is **1.96.0** (edition 2024).

## Example

Common types (`Options`, `MaybeResolved`, delegates, …) are re-exported from the
per-runtime module; the runtime-pinned constructors (`tcp` / `tls` / `quic`) live there
too (`memberlist::tokio`, `memberlist::smol`, `memberlist::compio`).

```rust,ignore
use core::net::SocketAddr;
use memberlist::tokio::{tcp, MaybeResolved, Options, SocketAddrResolver, VoidDelegate};
use smol_str::SmolStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let advertise: SocketAddr = "127.0.0.1:7946".parse()?;

    // Start a node pinned to the tokio runtime (TCP reliable plane + UDP gossip).
    let node = tcp(
        &SocketAddrResolver,
        SmolStr::new("node-a"),
        MaybeResolved::Resolved(advertise),
        Options::new(),
        VoidDelegate::<SmolStr, SocketAddr>::new(),
    )
    .await?;

    // Join an existing cluster through one or more seed addresses.
    let seed: SocketAddr = "127.0.0.1:7947".parse()?;
    node.join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed)])
        .await?;

    println!("{} members online", node.num_online_members());

    // Gracefully leave: broadcast a leave, then drain.
    node.leave().await?;
    Ok(())
}
```

## Feature flags

Pick **one** runtime, **one or more** transports, and any transforms you need.

- **Runtimes** — `tokio` *(default)*, `smol`, `compio` (thread-per-core), `reactor`
  (generic over an [`agnostic`] runtime), `smoltcp` / `embassy` / `embedded` (`no_std`).
- **Transports** — `tcp` *(default)*; `tls` + a backend (`tls-rustls-ring`,
  `tls-rustls-aws-lc-rs`); `quic` + a backend (`quic-rustls-ring`,
  `quic-rustls-aws-lc-rs`).
- **Compression** (both planes) — `lz4`, `snappy`, `zstd`, `brotli`.
- **Encryption** (gossip + plain-TCP reliable, AEAD) — `aes-gcm`, `chacha20-poly1305`.
- **Checksum** (gossip plane) — `crc32`, `xxhash64`, `xxhash32`, `xxhash3`, `murmur3`.
- **Config** — `serde` (config-file round-trips) and `clap` (CLI flags + env) on the
  `*Options` types; std-only.
- **Other** — `cidr` (IP allow-list admission), `dns` (DNS address resolver), `getifs` (auto-detect the advertise address from local interfaces), `tracing`.

Compression applies on both planes; AEAD encryption applies on the gossip plane and on
plain-TCP reliable streams (QUIC and TLS reliable streams are already secure, so it is
skipped there); checksum applies on the gossip plane only.

## Observability

Enable `features = ["tracing"]` and install a subscriber in `main`:

```rust,ignore
fn main() {
    tracing_subscriber::fmt().init();
    // … start your node …
}
```

The driver task and `memberlist-proto` emit `tracing` events during probing, suspicion,
gossip dissemination, push/pull anti-entropy, and join / leave.

## License

`memberlist` is under the terms of the MPL-2.0 license.

See [LICENSE] for details.

Copyright (c) 2025 Al Liu.

Copyright (c) 2013 HashiCorp, Inc.

[HashiCorp's memberlist]: https://github.com/hashicorp/memberlist
[`agnostic`]: https://github.com/al8n/agnostic
[`memberlist`]: https://crates.io/crates/memberlist
[`memberlist-proto`]: https://crates.io/crates/memberlist-proto
[`memberlist-reactor`]: https://crates.io/crates/memberlist-reactor
[`memberlist-compio`]: https://crates.io/crates/memberlist-compio
[`memberlist-embedded`]: https://crates.io/crates/memberlist-embedded
[`memberlist-smoltcp`]: https://crates.io/crates/memberlist-smoltcp
[`memberlist-embassy`]: https://crates.io/crates/memberlist-embassy
[LICENSE]: https://github.com/al8n/memberlist/blob/main/LICENSE
[Github-url]: https://github.com/al8n/memberlist/
[CI-url]: https://github.com/al8n/memberlist/actions/workflows/coverage.yml
[codecov-url]: https://app.codecov.io/gh/al8n/memberlist/
[doc-url]: https://docs.rs/memberlist
[crates-url]: https://crates.io/crates/memberlist
