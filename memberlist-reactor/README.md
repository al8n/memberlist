<div align="center">
<h1>memberlist-reactor</h1>
</div>
<div align="center">

Runtime-agnostic async **SWIM** membership driver — drives the Sans-I/O memberlist core
over `tokio` or `smol` via [`agnostic`].

[<img alt="github" src="https://img.shields.io/badge/github-al8n/memberlist-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/memberlist/coverage.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/memberlist?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-memberlist--reactor-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/memberlist-reactor?style=for-the-badge&logo=rust" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/memberlist-reactor?color=critical&logo=rust&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=mozilla" height="22">

</div>

## Introduction

`memberlist-reactor` drives the Sans-I/O core ([`memberlist-proto`]) over a
readiness-based ("reactor") async runtime. It is generic over an [`agnostic`] `Runtime`,
so the same driver runs on `tokio` and `smol` with no change to protocol behavior.

If you would rather not name the runtime type parameter, use the [`memberlist`] facade —
its `memberlist::tokio` / `memberlist::smol` modules are thin wrappers that pin the
runtime for you.

## Installation

```toml
[dependencies]
memberlist-reactor = "0.0.1"
```

## Example

```rust,ignore
use core::net::SocketAddr;
use agnostic::tokio::TokioRuntime;
use memberlist_reactor::{MaybeResolved, Memberlist, Options, SocketAddrResolver, VoidDelegate};
use smol_str::SmolStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let advertise: SocketAddr = "127.0.0.1:7946".parse()?;

    // The runtime is a type parameter; pick `TokioRuntime` or `SmolRuntime`.
    let node = Memberlist::<SmolStr>::tcp::<TokioRuntime, _, _>(
        &SocketAddrResolver,
        SmolStr::new("node-a"),
        MaybeResolved::Resolved(advertise),
        Options::new(),
        VoidDelegate::<SmolStr, SocketAddr>::new(),
    )
    .await?;

    let seed: SocketAddr = "127.0.0.1:7947".parse()?;
    node.join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed)])
        .await?;

    println!("{} members online", node.num_online_members());

    node.leave().await?;
    Ok(())
}
```

## Feature flags

| Feature | Description |
|---------|-------------|
| `tcp` | plain TCP reliable streams + UDP gossip |
| `tls` + `tls-rustls-ring` / `tls-rustls-aws-lc-rs` | TLS-over-TCP via `rustls` |
| `quic` + `quic-rustls-ring` / `quic-rustls-aws-lc-rs` | QUIC reliable streams + datagrams |
| `compression-lz4` / `-snappy` / `-zstd` / `-brotli` | gossip-plane compression |
| `encryption-aes-gcm` / `-chacha20-poly1305` | gossip-plane AEAD encryption |
| `checksum-crc32` / `-xxhash64` / … | gossip-plane checksum |
| `cidr` | IP allow-list admission |
| `getifs` | auto-detect the advertise address from the host's interfaces (`LocalAddrResolver`) |
| `tracing` | forward structured `tracing` spans/events from the driver and `memberlist-proto` |

## Design

- A quinn-style poll-pump driver: one spawned task per endpoint pumps the
  [`memberlist-proto`] super-machine — feeding inbound packets / stream events and timers,
  draining `poll_transmit` / `poll_event` — over the runtime's sockets.
- A command queue + waker bridges the public `Memberlist` handle to the driver task; the
  handle's membership queries read a snapshot without blocking the loop.
- Owns its own `AddressResolver` boundary, so resolution stays outside the Sans-I/O core.

## The memberlist family

[`memberlist`] (facade) · [`memberlist-proto`] (Sans-I/O core) ·
**`memberlist-reactor`** (this crate) · [`memberlist-compio`] (compio driver) ·
[`memberlist-embedded`] (shared `no_std` core) · [`memberlist-smoltcp`] (smoltcp driver) ·
[`memberlist-embassy`] (embassy driver).

## License

`memberlist-reactor` is under the terms of the MPL-2.0 license.

See [LICENSE] for details.

Copyright (c) 2025 Al Liu.

Copyright (c) 2013 HashiCorp, Inc.

[`agnostic`]: https://github.com/al8n/agnostic
[`memberlist`]: https://crates.io/crates/memberlist
[`memberlist-proto`]: https://crates.io/crates/memberlist-proto
[`memberlist-compio`]: https://crates.io/crates/memberlist-compio
[`memberlist-embedded`]: https://crates.io/crates/memberlist-embedded
[`memberlist-smoltcp`]: https://crates.io/crates/memberlist-smoltcp
[`memberlist-embassy`]: https://crates.io/crates/memberlist-embassy
[LICENSE]: https://github.com/al8n/memberlist/blob/main/LICENSE
[Github-url]: https://github.com/al8n/memberlist/
[CI-url]: https://github.com/al8n/memberlist/actions/workflows/coverage.yml
[codecov-url]: https://app.codecov.io/gh/al8n/memberlist/
[doc-url]: https://docs.rs/memberlist-reactor
[crates-url]: https://crates.io/crates/memberlist-reactor
