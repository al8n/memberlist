<div align="center">

<img src="https://raw.githubusercontent.com/al8n/memberlist/main/art/logo.png" height = "200px">

<h1>Memberlist</h1>

</div>
<div align="center">

Batteries-included, runtime-agnostic, WASM/WASI-friendly **SWIM** gossip membership and
failure detection for Rust — a Sans-I/O protocol core with pluggable async drivers.

Port and improve [HashiCorp's memberlist] to Rust.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/memberlist-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
<img alt="LoC" src="https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fal8n%2Fd29ceff54c025fe4e8b144a51efb9324%2Fraw%2Fmemberlist" height="22">
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/memberlist/coverage.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/memberlist?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-memberlist-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/memberlist?style=for-the-badge&logo=rust" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/memberlist?color=critical&logo=rust&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPHN2ZyBpZD0iX+WbvuWxgl8xIiBkYXRhLW5hbWU9IuWbvuWxgiAxIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA0NzMuNDcgMjU1LjEyMiI+CiAgPGRlZnM+CiAgICA8c3R5bGU+CiAgICAgIC5jbHMtMSB7CiAgICAgICAgZmlsbDogI2ZmZjsKICAgICAgICBzdHJva2Utd2lkdGg6IDBweDsKICAgICAgfQogICAgPC9zdHlsZT4KICA8L2RlZnM+CiAgPHBvbHlnb24gY2xhc3M9ImNscy0xIiBwb2ludHM9IjM0MC4wNjUgLjQ4NCAzMzQuMzczIDEuMjA5IDMyOC45MjkgMi4xNzUgMzIzLjczMSAzLjYyOCAzMTguNzggNS4zMTggMzEzLjgzMiA3LjAxMiAzMDkuMTI3IDkuMTkgMzA0LjY3MiAxMS42MDYgMzAwLjQ2NiAxNC4yNjggMjk2LjI1OSAxNy4xNjkgMjkyLjI5NyAyMC4zMTIgMjg4LjU4NiAyMy42OTcgMjg1LjEyIDI3LjMyNiAyODEuOTAyIDMxLjE5NCAyNzguNjg0IDM1LjMwNyAyNzUuOTYyIDM5LjQxNiAyNzMuMjQgNDQuMDExIDI3MC43NjUgNDguNjA3IDI2OC41MzkgNTMuNjg1IDI2Ni41NTkgNDguMzYzIDI2NC4wODMgNDMuMjg1IDI2MS42MDggMzguNDUxIDI1OC42MzkgMzMuODU0IDI1NS40MjIgMjkuNzQ0IDI1MS45NTUgMjUuODc1IDI0OC4yNDQgMjIuMjQ3IDI0NC41MzEgMTguODYzIDI0MC4zMjIgMTUuNzE5IDIzNi4xMTYgMTMuMDU5IDIzMS40MTQgMTAuMzk3IDIyNi45NTkgOC4yMjIgMjIyLjAwOCA2LjI4OCAyMTcuMDU3IDQuNTk0IDIxMi4xMDkgMy4xNDMgMjA2LjkxMSAxLjkzNCAyMDEuNDY0IC45NjggMTkwLjU3NiAwIDE3OS45MzIgMCAxNzQuNzM1IC40ODQgMTY5Ljc4NiAuOTY4IDE2NC44MzUgMS42OTMgMTYwLjEzNCAyLjkwMyAxNTUuNjc4IDQuMTEyIDE1MS4yMjMgNS41NjIgMTQ2Ljc2NyA3LjI1MyAxNDIuODA3IDkuMTkgMTM4LjYwMiAxMS4xMjUgMTM0Ljg4OCAxMy41NDEgMTMxLjE3NSAxNS45NiAxMjcuNzExIDE4LjYxOSAxMjQuMjQ0IDIxLjUyMiAxMjEuMDI5IDI0LjY2NiAxMTguMDU4IDI3LjgwOSAxMTUuMDg3IDMxLjE5NCAxMTIuMzY1IDM0LjgyMiAxMDkuODg5IDM4LjY5MSAxMDcuNjYzIDQyLjU2IDEwNy42NjMgNS4wNzggMCA1LjA3OCAwIDU4Ljc2NCAzMy45MDcgNTguNzY0IDMzLjkwNyAyMDAuNDcgMCAyMDAuNDcgMCAyNTUuMTIyIDE1Ni42NjcgMjU1LjEyMiAxNTYuNjY3IDIwMC40NyAxMDcuNjYzIDIwMC40NyAxMDcuNjYzIDEwOC4zMzcgMTA3LjkwOSAxMDMuMjU5IDEwOC42NTIgOTguNDIxIDEwOS4zOTYgOTMuODI3IDExMC4zODUgODkuNDc0IDExMS42MjMgODUuMTIxIDExMy4xMDcgODEuMjUyIDExNC44NCA3Ny4zODMgMTE2LjgyIDczLjk5OCAxMTkuMDQ3IDcwLjYxMSAxMjEuNzcyIDY3LjcxMSAxMjQuNDk0IDY1LjA1MSAxMjcuNDYxIDYyLjYzMyAxMzAuOTI5IDYwLjQ1NSAxMzQuNjM5IDU4LjUyIDEzOC4zNTIgNTcuMDcgMTQyLjU2MSA1NS44NiAxNDcuMjYzIDU0Ljg5NSAxNTEuOTY1IDU0LjQxIDE1Ny4xNjIgNTQuMTY3IDE2MS4zNzEgNTQuMTY3IDE2NS4zMzEgNTQuNjUxIDE2OS4yOTEgNTUuMzc2IDE3Mi43NTUgNTYuMzQ1IDE3Ni4yMjEgNTcuNTU0IDE3OS40MzkgNTkuMDA1IDE4Mi40MDggNjAuNjk4IDE4NS4xMyA2Mi44NzMgMTg3LjYwNSA2NS4yOTIgMTkwLjA4MSA2Ny45NTIgMTkyLjA2MSA3MC44NTQgMTk0LjA0MSA3NC4yMzkgMTk1LjUyNCA3OC4xMDggMTk3LjAxMSA4MS45NzcgMTk4LjI0OSA4Ni41NzQgMTk5LjIzOCA5MS4xNjcgMTk5Ljk4IDk2LjI0NiAyMDAuNzIyIDEwMS44MDkgMjAwLjk3MSAxMDcuODUyIDIwMC45NzEgMjU1LjEyMiAzMDcuMzk3IDI1NS4xMjIgMzA3LjM5NyAyMDAuNDcgMjczLjQ4NyAyMDAuNDcgMjczLjQ4NyAxMTMuNDE1IDI3My43MzYgMTA4LjMzNyAyNzMuOTgzIDEwMy4yNTkgMjc0LjQ3OCA5OC40MjEgMjc1LjQ2NiA5My44MjcgMjc2LjQ1OCA4OS40NzQgMjc3LjY5NiA4NS4xMjEgMjc5LjE4IDgxLjI1MiAyODAuOTEzIDc3LjM4MyAyODIuODk0IDczLjk5OCAyODUuMTIgNzAuNjExIDI4Ny41OTUgNjcuNzExIDI5MC41NjcgNjUuMDUxIDI5My41MzQgNjIuNjMzIDI5Ny4wMDIgNjAuNDU1IDMwMC40NjYgNTguNTIgMzA0LjQyNSA1Ny4wNyAzMDguNjM1IDU1Ljg2IDMxMy4zMzYgNTQuODk1IDMxOC4wMzggNTQuNDEgMzIzLjIzNSA1NC4xNjcgMzI3LjQ0NCA1NC4xNjcgMzMxLjQwNCA1NC42NTEgMzM1LjM2NCA1NS4zNzYgMzM4LjgyOCA1Ni4zNDUgMzQyLjI5MiA1Ny41NTQgMzQ1LjUwOSA1OS4wMDUgMzQ4LjQ4MSA2MC42OTggMzUxLjIwMyA2Mi44NzMgMzUzLjY3OCA2NS4yOTIgMzU1LjkwNCA2Ny45NTIgMzU4LjEzMyA3MC44NTQgMzYwLjExNCA3NC4yMzkgMzYzLjA4MiA4MS45NzcgMzY0LjMyIDg2LjU3NCAzNjUuMzExIDkxLjE2NyAzNjYuMDUzIDk2LjI0NiAzNjYuNzk1IDEwMS44MDkgMzY3LjA0NCAxMDcuODUyIDM2Ny4wNDQgMjU1LjEyMiA0NzMuNDcgMjU1LjEyMiA0NzMuNDcgMjAwLjQ3IDQzOS41NiAyMDAuNDcgNDM5LjU2IDg2LjMzIDQzOS4zMTMgNzcuNjI0IDQzOC4zMjIgNjkuNjQ1IDQzNi44MzggNjEuOTA3IDQzNC44NTggNTQuNjUxIDQzMi4zODMgNDcuODgyIDQyOS40MTQgNDEuNTk1IDQyNS45NDggMzUuNzg4IDQyMS45ODggMzAuMjI5IDQxNy41MzIgMjUuMzkxIDQxMy4wNzcgMjAuNzk3IDQwNy44OCAxNi45MjggNDAyLjY4MiAxMy4zIDM5Ni45OTEgMTAuMTU3IDM5MS4wNTIgNy4yNTMgMzg0Ljg2MyA1LjA3OCAzNzguNjc0IDMuMTQzIDM3MS45OTMgMS42OTMgMzY1LjU1OCAuNzI1IDM1OC42MjkgMCAzNDUuNzU5IDAgMzQwLjA2NSAuNDg0Ii8+Cjwvc3ZnPg==" height="22">

[<img alt="Discord" src="https://img.shields.io/discord/835936528140206122?style=for-the-badge&logo=discord&logoColor=white&label=Discord&color=7289da" height="22">][discord]

</div>

## Introduction

`memberlist` manages cluster membership and member failure detection using a gossip-based
protocol — the foundation any distributed system needs. It is eventually consistent but
converges quickly (and the convergence rate is tunable through the protocol's knobs), and it
tolerates network partitions by attempting to reach potentially-dead nodes through multiple
routes.

Its protocol logic is a runtime-agnostic **Sans-I/O** state machine ([`memberlist-proto`]),
modeled on [`quinn-proto`]; thin async drivers adapt it to `tokio` / `smol`, `compio`, and
bare-metal `no_std` targets, so the same SWIM core runs on a server or a microcontroller. The
`memberlist` crate is the high-level entry point: it wires that core to a ready-to-use async
driver ([`memberlist-reactor`]) so you can join a cluster in a few lines, with `tokio` out of
the box.

This is a Rust port of [HashiCorp's memberlist], extended with a Sans-I/O architecture and
`no_std` / bare-metal support. Every crate is WASM/WASI friendly and can compile to
`wasm32-unknown-unknown` and `wasm32-wasip1` with the appropriate features.

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

> **Build requirement:** `memberlist-proto`'s build script invokes [`protoc`][protoc] to
> generate the wire codec, so the Protocol Buffers compiler must be on `PATH` when building
> (e.g. `apt install protobuf-compiler`, `brew install protobuf`). CI installs it via
> `arduino/setup-protoc`.

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

For bare-metal (`no_std`) targets, enable `smoltcp` (the executor-free engine) or `embassy`
(the embassy-net async driver) — neither pulls in `std`:

```toml
[dependencies]
memberlist = { version = "0.9", default-features = false, features = ["embassy", "tcp"] }
```

The minimum supported Rust version (MSRV) is **1.96.0** (edition 2024).

## Example

Common types (`Options`, `MaybeResolved`, delegates, …) are re-exported from the per-runtime
module; the runtime-pinned constructors (`tcp` / `tls` / `quic`) live there too
(`memberlist::tokio`, `memberlist::smol`, `memberlist::compio`).

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
- **Other** — `cidr` (IP allow-list admission), `dns` (DNS address resolver), `getifs`
  (auto-detect the advertise address from local interfaces), `tracing`.

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

## Protocol

memberlist is based on ["SWIM: Scalable Weakly-consistent Infection-style Process Group
Membership Protocol"](http://ieeexplore.ieee.org/document/1028914/). HashiCorp's developers
extended the protocol in a number of ways: several extensions increase propagation speed and
convergence rate, and a further set — which they call Lifeguard — make memberlist more robust
in the presence of slow message processing (due to factors such as CPU starvation and network
delay or loss). For details, read HashiCorp's paper ["Lifeguard: SWIM-ing with Situational
Awareness"](https://arxiv.org/abs/1707.00788) alongside the memberlist source.

## Design

Unlike the original Go implementation, the Rust memberlist uses a highly generic, layered
architecture: you can implement a component yourself and plug it in, and you can even bring
your own `Id` and `Address`. The layers are:

- **Transport drivers**

  The protocol logic is a runtime-agnostic Sans-I/O state machine ([`memberlist-proto`],
  modeled on [`quinn-proto`]). Each driver pairs that core with one async runtime; protocol
  behavior is identical across all of them — select the one matching your runtime from
  [the family](#the-family) above, or depend on a driver crate directly. Every driver carries
  three transports — plain **TCP**, **TLS-over-TCP** (`rustls`), and **QUIC** (`quinn-proto`)
  — each a reliable stream plane plus a UDP / datagram gossip plane. The runtime layer is
  provided by [`agnostic`'s `Runtime`](https://docs.rs/agnostic/latest/agnostic/trait.Runtime.html)
  (for the reactor driver), and each driver provides its own `AddressResolver` trait (with
  built-in resolvers such as `SocketAddrResolver`). You can bring your own `Id`, `Address`,
  and `AddressResolver`.

- **Delegate layer**

  This layer is used as a reactor for different kinds of messages.

  - **`Delegate`** is the trait clients implement to hook into the gossip layer. All methods
    must be thread-safe, as they can and generally will be called concurrently. It is split
    into focused sub-traits:

    - **`AliveDelegate`** — involve a client in processing a node "alive" message. When a node
      joins (through packet gossip or promised push/pull), its state is updated via an alive
      message; this hook can filter a node out using application-specific logic.
    - **`ConflictDelegate`** — inform a client that a joining node would cause a name conflict
      (two clients configured with the same name but different addresses).
    - **`EventDelegate`** — a simpler delegate that only receives notifications about members
      joining and leaving. Its methods may be called by multiple threads, but never
      concurrently, so you can reason about ordering.
    - **`MergeDelegate`** — involve a client in a potential cluster merge: on every promised
      push/pull — both the initial join and ongoing anti-entropy — the delegate is consulted
      and may veto the exchange. (This deliberately tightens HashiCorp's memberlist, which
      consults the merge delegate only on join.)
    - **`NodeDelegate`** — manage node-related events, e.g. metadata.
    - **`PingDelegate`** — notify an observer how long a ping round trip took, and write
      arbitrary bytes into ack messages. To stay meaningful for RTT estimates, it does not
      apply to indirect pings or to fallback pings sent over a promised connection.

  - **`CompositeDelegate`** splits the `Delegate` into multiple small delegates, so you don't
    have to implement the full `Delegate` when you only want to customize a few methods.

## Projects using memberlist

- [serf](https://github.com/al8n/serf): a decentralized solution for service discovery and
  orchestration that is lightweight, highly available, and fault tolerant.
- [examples/toydb](https://github.com/al8n/memberlist/tree/main/examples/toydb): a toy
  eventually-consistent distributed key-value database.

## Q & A

- ***Is the Rust memberlist implementation compatible with Go's memberlist?***

  No, but yes! The Rust implementation uses a protobuf-like forward- and backward-compatible
  encoding, whereas Go's uses MessagePack. Interop is possible in theory — you would need to
  implement your own transport layer — but it is not recommended; you would face expensive
  overhead.

- ***If Go's memberlist adds more functionality, will this project support it too?***

  Yes! And this project may also add functionality that Go's memberlist does not have, e.g.
  WASM support and bindings to other languages.

## Related Projects

- [`agnostic`](https://github.com/al8n/agnostic): helps you develop runtime-agnostic crates.

## License

`memberlist` is under the terms of the MPL-2.0 license. See [LICENSE] for details.

Copyright (c) 2025 Al Liu.

Copyright (c) 2013 HashiCorp, Inc.

[HashiCorp's memberlist]: https://github.com/hashicorp/memberlist
[`quinn-proto`]: https://docs.rs/quinn-proto
[`agnostic`]: https://github.com/al8n/agnostic
[`memberlist`]: https://crates.io/crates/memberlist
[`memberlist-proto`]: https://crates.io/crates/memberlist-proto
[`memberlist-reactor`]: https://crates.io/crates/memberlist-reactor
[`memberlist-compio`]: https://crates.io/crates/memberlist-compio
[`memberlist-embedded`]: https://crates.io/crates/memberlist-embedded
[`memberlist-smoltcp`]: https://crates.io/crates/memberlist-smoltcp
[`memberlist-embassy`]: https://crates.io/crates/memberlist-embassy
[protoc]: https://grpc.io/docs/protoc-installation/
[LICENSE]: https://github.com/al8n/memberlist/blob/main/LICENSE
[Github-url]: https://github.com/al8n/memberlist/
[CI-url]: https://github.com/al8n/memberlist/actions/workflows/coverage.yml
[codecov-url]: https://app.codecov.io/gh/al8n/memberlist/
[doc-url]: https://docs.rs/memberlist
[crates-url]: https://crates.io/crates/memberlist
[discord]: https://discord.gg/f3EtWBM7ke
