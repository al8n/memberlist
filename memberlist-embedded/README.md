<div align="center">
<h1>memberlist-embedded</h1>
</div>
<div align="center">

Transport-agnostic `no_std` driving core shared by the embedded **SWIM** membership
drivers.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/memberlist-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
<img alt="LoC" src="https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fal8n%2Fd29ceff54c025fe4e8b144a51efb9324%2Fraw%2Fmemberlist-embedded" height="22">
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/memberlist/coverage.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/memberlist?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-memberlist--embedded-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/memberlist-embedded?style=for-the-badge&logo=rust" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/memberlist-embedded?color=critical&logo=rust&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=mozilla" height="22">

</div>

## Introduction

`memberlist-embedded` is the neutral `no_std` core that the embedded drivers build on. It
wires the Sans-I/O machine ([`memberlist-proto`]) to a transport through two small,
non-blocking I/O seams — a `GossipIo` for the unreliable plane and a `StreamIo` for the
reliable plane — and adds the generic reliable-plane state machine plus the outbound
transform / gossip-codec pipeline.

It owns no executor and performs no blocking: you implement the I/O traits for your
network stack and pump the `Engine` from your own loop. This is the shared engine that
[`memberlist-smoltcp`] (caller-poll) and [`memberlist-embassy`] (async) build on — most
applications want one of those, which provide the I/O implementations for you, rather
than this core directly.

## Feature tiers

| Features | Environment |
|----------|-------------|
| `std` *(default)* | host builds and the test harness |
| `alloc` | `no_std` with a global allocator (bare metal) |

Build bare-metal with `--no-default-features --features alloc`. The protocol state lives
in slab-backed pools, so there is no per-packet heap traffic on the hot path.

Opt-in transforms (none enabled by default): `lz4` / `-snappy` / `-zstd` /
`-brotli` compression and `aes-gcm` / `-chacha20-poly1305` AEAD encryption apply
on both the gossip and (plain-TCP) reliable planes; `crc32` / `-xxhash64` /
`-xxhash32` / `-xxhash3` / `-murmur3` checksum is gossip-plane only. `cidr` adds
an IP allow-list usable as an `AliveDelegate`.

## Installation

```toml
[dependencies]
memberlist-embedded = { version = "0.1", default-features = false, features = ["alloc"] }
```

## The memberlist family

[`memberlist`] (facade) · [`memberlist-proto`] (Sans-I/O core) ·
[`memberlist-reactor`] (tokio / smol driver) · [`memberlist-compio`] (compio driver) ·
**`memberlist-embedded`** (this crate) · [`memberlist-smoltcp`] (smoltcp driver) ·
[`memberlist-embassy`] (embassy driver).

## License

`memberlist-embedded` is under the terms of the MPL-2.0 license.

See [LICENSE] for details.

Copyright (c) 2025 Al Liu.

Copyright (c) 2013 HashiCorp, Inc.

[`memberlist`]: https://crates.io/crates/memberlist
[`memberlist-proto`]: https://crates.io/crates/memberlist-proto
[`memberlist-reactor`]: https://crates.io/crates/memberlist-reactor
[`memberlist-compio`]: https://crates.io/crates/memberlist-compio
[`memberlist-smoltcp`]: https://crates.io/crates/memberlist-smoltcp
[`memberlist-embassy`]: https://crates.io/crates/memberlist-embassy
[LICENSE]: https://github.com/al8n/memberlist/blob/main/LICENSE
[Github-url]: https://github.com/al8n/memberlist/
[CI-url]: https://github.com/al8n/memberlist/actions/workflows/coverage.yml
[codecov-url]: https://app.codecov.io/gh/al8n/memberlist/
[doc-url]: https://docs.rs/memberlist-embedded
[crates-url]: https://crates.io/crates/memberlist-embedded
