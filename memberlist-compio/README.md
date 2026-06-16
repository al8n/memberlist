<div align="center">
<h1>memberlist-compio</h1>
</div>
<div align="center">

`compio`-native async **SWIM** membership — responder, querier, and failure detection on
a completion-based (io_uring / IOCP), thread-per-core runtime.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/memberlist-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/memberlist/coverage.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/memberlist?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-memberlist--compio-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/memberlist-compio?style=for-the-badge&logo=rust" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/memberlist-compio?color=critical&logo=rust&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=mozilla" height="22">

</div>

## Introduction

`memberlist-compio` drives the Sans-I/O core ([`memberlist-proto`]) over [`compio`], a
completion-based (io_uring on Linux, IOCP on Windows) thread-per-core runtime.

For the `tokio` / `smol` runtimes, use [`memberlist-reactor`] or the [`memberlist`]
facade instead.

## Installation

```toml
[dependencies]
memberlist-compio = "0.0.1"
```

## Example

Run inside a `compio` runtime (the driver is `!Send`, thread-per-core):

```rust,ignore
use core::net::SocketAddr;
use memberlist_compio::{
    FirstAddrResolver, MaybeResolved, Memberlist, Options, SocketAddrResolver,
    TcpTransport, TcpTransportOptions, VoidDelegate,
};
use smol_str::SmolStr;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let advertise: SocketAddr = "127.0.0.1:7946".parse()?;

    let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
        TcpTransportOptions::<SmolStr, SocketAddr>::new()
            .with_local_id(SmolStr::new("node-a"))
            .with_advertise_addr(MaybeResolved::Resolved(advertise)),
    );
    let node = Memberlist::new(
        opts,
        VoidDelegate::default(),
        &SocketAddrResolver,
        &FirstAddrResolver,
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
| `lz4` / `-snappy` / `-zstd` / `-brotli` | gossip-plane compression |
| `aes-gcm` / `-chacha20-poly1305` | gossip-plane AEAD encryption |
| `crc32` / `-xxhash64` / … | gossip-plane checksum |
| `cidr` | IP allow-list admission |
| `dns` | DNS address resolution (via `hickory-proto`) |
| `getifs` | auto-detect the advertise address from the host's interfaces (`LocalAddrResolver`) |
| `tracing` | forward structured `tracing` spans/events from the driver and `memberlist-proto` |

## Observability

Enable `features = ["tracing"]` to have the driver emit `tracing` events during socket
I/O, probing, suspicion, and join / leave.

## Design

- `!Send` thread-per-core: no `Arc`, no `Mutex`, no cross-thread channels on the hot path.
- One spawned driver task per endpoint owns the compio `TcpListener` + `UdpSocket`(s) and
  pumps the [`memberlist-proto`] super-machine over completion-based I/O.
- Public handles hold `Rc<…>` and read membership snapshots under short, non-`.await`
  borrows.

## The memberlist family

[`memberlist`] (facade) · [`memberlist-proto`] (Sans-I/O core) ·
[`memberlist-reactor`] (tokio / smol driver) · **`memberlist-compio`** (this crate) ·
[`memberlist-embedded`] (shared `no_std` core) · [`memberlist-smoltcp`] (smoltcp driver) ·
[`memberlist-embassy`] (embassy driver).

## License

`memberlist-compio` is under the terms of the MPL-2.0 license.

See [LICENSE] for details.

Copyright (c) 2025 Al Liu.

Copyright (c) 2013 HashiCorp, Inc.

[`compio`]: https://crates.io/crates/compio
[`memberlist`]: https://crates.io/crates/memberlist
[`memberlist-proto`]: https://crates.io/crates/memberlist-proto
[`memberlist-reactor`]: https://crates.io/crates/memberlist-reactor
[`memberlist-embedded`]: https://crates.io/crates/memberlist-embedded
[`memberlist-smoltcp`]: https://crates.io/crates/memberlist-smoltcp
[`memberlist-embassy`]: https://crates.io/crates/memberlist-embassy
[LICENSE]: https://github.com/al8n/memberlist/blob/main/LICENSE
[Github-url]: https://github.com/al8n/memberlist/
[CI-url]: https://github.com/al8n/memberlist/actions/workflows/coverage.yml
[codecov-url]: https://app.codecov.io/gh/al8n/memberlist/
[doc-url]: https://docs.rs/memberlist-compio
[crates-url]: https://crates.io/crates/memberlist-compio
