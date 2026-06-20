<div align="center">
<h1>memberlist-proto</h1>
</div>
<div align="center">

Sans-I/O state machine for the **SWIM** gossip membership protocol — `no_std`-capable,
modeled on [`quinn-proto`].

[<img alt="github" src="https://img.shields.io/badge/github-al8n/memberlist-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/memberlist/coverage.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/memberlist?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-memberlist--proto-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/memberlist-proto?style=for-the-badge&logo=rust" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/memberlist-proto?color=critical&logo=rust&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=mozilla" height="22">

</div>

## Introduction

`memberlist-proto` implements HashiCorp's [memberlist] SWIM protocol — gossip-based
cluster membership, failure detection, and the Lifeguard extensions — as deterministic
state machines that perform **no I/O of their own**.

You feed inbound packets / streams and timer ticks in via `Endpoint::handle*` /
`Endpoint::handle_timeout`, and drain outbound work via `Endpoint::poll_transmit` /
`Endpoint::poll_event` — the same Sans-I/O shape as [`quinn-proto`]. This keeps the
protocol logic portable and exhaustively testable: the same core runs under any async
runtime, on bare metal, or inside a fuzzer.

The async driver crates in the memberlist family ([`memberlist-reactor`],
[`memberlist-compio`], plus the bare-metal [`memberlist-smoltcp`] /
[`memberlist-embassy`]) are thin layers over this crate. Most applications want one of
those, or the [`memberlist`] facade, rather than this core directly.

## Feature tiers

`std` and `alloc` are **independent** features, so the crate scales from a full `std`
host down to bare metal with an allocator:

| Features | Environment |
|----------|-------------|
| `std` *(default)* | `std` hosts (tokio, smol, compio) |
| `alloc` | `no_std` with a global allocator (embassy, smoltcp, …) |
| *(none)* | bare-metal `no_std`, no heap |

The core machine and the plain-`tcp` stream coordinator are `no_std` + `alloc`; the
`quic` (quinn-proto) and `tls` (rustls) coordinators require `std`.

## Transports & transforms

Each transport is a composed Sans-I/O coordinator carrying both the reliable stream
plane and the unreliable gossip plane:

| Feature | Transport |
|---------|-----------|
| `tcp` | plain TCP reliable streams + UDP gossip (label-framed) |
| `tls` + a backend (`tls-rustls-ring`, `tls-rustls-aws-lc-rs`, …) | rustls-over-TCP, with the record layer inside the coordinator |
| `quic` + a backend (`quic-rustls-ring`, `quic-aws-lc-rs`, …) | quinn-proto streams + datagrams |

Opt-in transforms apply on the unreliable gossip plane (none are enabled by default):

| Kind | Features |
|------|----------|
| Checksum | `crc32`, `xxhash64`, `xxhash32`, `xxhash3`, `murmur3` |
| Compression | `lz4`, `snappy`, `zstd`, `brotli` |
| Encryption (AEAD) | `aes-gcm`, `chacha20-poly1305` |

`cidr` adds a `CidrPolicy` IP allow-list usable as an `AliveDelegate` for membership
admission.

The config `Options` types optionally derive `serde` (config-file round-trips) and `clap`
(CLI flags + env) under the `serde` / `clap` features — both std-only.

## Installation

```toml
[dependencies]
memberlist-proto = "0.4"                                                # std (default)

# no_std + alloc, with the plain-tcp coordinator:
memberlist-proto = { version = "0.4", default-features = false, features = ["alloc", "tcp"] }

# bare no_std, no allocator:
memberlist-proto = { version = "0.4", default-features = false }
```

## Observability

Enable `features = ["tracing"]` to have the state machine emit structured `tracing`
events during probing, suspicion, gossip dissemination, and push/pull anti-entropy.
Wire a subscriber in your application:

```rust,ignore
tracing_subscriber::fmt().init();
```

## The memberlist family

The crates split protocol logic from I/O, mirroring the `quinn` layering:

[`memberlist`] (facade) · **`memberlist-proto`** (this crate) ·
[`memberlist-reactor`] (tokio / smol driver) · [`memberlist-compio`] (compio driver) ·
[`memberlist-embedded`] (shared `no_std` core) · [`memberlist-smoltcp`] (smoltcp driver) ·
[`memberlist-embassy`] (embassy driver).

## License

`memberlist-proto` is under the terms of the MPL-2.0 license.

See [LICENSE] for details.

Copyright (c) 2025 Al Liu.

Copyright (c) 2013 HashiCorp, Inc.

[memberlist]: https://github.com/hashicorp/memberlist
[`quinn-proto`]: https://crates.io/crates/quinn-proto
[`memberlist`]: https://crates.io/crates/memberlist
[`memberlist-reactor`]: https://crates.io/crates/memberlist-reactor
[`memberlist-compio`]: https://crates.io/crates/memberlist-compio
[`memberlist-embedded`]: https://crates.io/crates/memberlist-embedded
[`memberlist-smoltcp`]: https://crates.io/crates/memberlist-smoltcp
[`memberlist-embassy`]: https://crates.io/crates/memberlist-embassy
[LICENSE]: https://github.com/al8n/memberlist/blob/main/LICENSE
[Github-url]: https://github.com/al8n/memberlist/
[CI-url]: https://github.com/al8n/memberlist/actions/workflows/coverage.yml
[codecov-url]: https://app.codecov.io/gh/al8n/memberlist/
[doc-url]: https://docs.rs/memberlist-proto
[crates-url]: https://crates.io/crates/memberlist-proto
