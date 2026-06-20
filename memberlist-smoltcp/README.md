<div align="center">
<h1>memberlist-smoltcp</h1>
</div>
<div align="center">

Executor-free `no_std` **SWIM** membership driver over the [smoltcp] TCP/IP stack — no OS
and no async runtime required.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/memberlist-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/memberlist/coverage.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/memberlist?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-memberlist--smoltcp-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/memberlist-smoltcp?style=for-the-badge&logo=rust" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/memberlist-smoltcp?color=critical&logo=rust&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=mozilla" height="22">

</div>

## Introduction

`memberlist-smoltcp` drives the Sans-I/O core ([`memberlist-proto`], through the shared
[`memberlist-embedded`] engine) over a [smoltcp] TCP/IP stack. It owns no executor and
performs no blocking: you pump it from your own poll loop alongside the smoltcp
`Interface`, and it tells you the next instant it wants to be polled.

`no_std` + `alloc`: the protocol state lives in slab-backed pools, so there is no
per-packet heap traffic on the hot path. For an async embedded driver, see
[`memberlist-embassy`]; for `std` hosts, see the [`memberlist`] facade.

## Installation

```toml
[dependencies]
memberlist-smoltcp = { version = "0.1", default-features = false, features = ["alloc"] }
```

## Example

```rust,ignore
use core::net::SocketAddr;
use memberlist_smoltcp::{
    EndpointOptions, InterfaceOptions, Memberlist, Options, TransformOptions,
};
use smol_str::SmolStr;

// `device` is your smoltcp `Device` (an Ethernet/IP driver); `now` is a portable
// `Instant` your firmware advances.
let advertise: SocketAddr = "10.0.0.2:7946".parse().unwrap();
let mut node = Memberlist::<SmolStr, _>::new(
    Options::new(),
    InterfaceOptions::new(hardware_addr), // + IP addresses, routes, RNG seed
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("node-a"), advertise),
    &mut device,
    now,
);

// Pump from your own loop; `poll` advances the protocol + the smoltcp interface and
// returns the next instant the driver wants to be polled (or `None`).
loop {
    let next = node.poll(now, &mut device);
    while let Some(event) = node.poll_event() {
        // react to membership events (joined / updated / left, ping completed, …)
    }
    // ...sleep until `next` or until the device is RX-ready, then advance `now`...
}
```

## Feature flags

| Feature | Description |
|---------|-------------|
| `std` *(default)* | host builds and the test harness |
| `alloc` | `no_std` with a global allocator (bare metal) — build with `--no-default-features --features alloc` |
| `lz4` / `-snappy` / `-zstd` / `-brotli` | compression (gossip + reliable) |
| `aes-gcm` / `-chacha20-poly1305` | AEAD encryption (gossip + plain-TCP reliable) |
| `crc32` / `-xxhash64` / … | gossip-plane checksum |
| `cidr` | IP allow-list admission |

## Design

- **`no_std` + `alloc`**, panic-free hot path: slab-backed protocol pools, no per-packet
  allocation.
- **Caller-poll**: no executor; you drive `poll(now, device)` from your loop and honor the
  returned next-wake instant — the same shape as advancing the smoltcp `Interface`.
- Built on the transport-agnostic [`memberlist-embedded`] `Engine`, with smoltcp UDP
  (gossip) + a TCP socket pool (reliable plane) behind its I/O seams.

## The memberlist family

[`memberlist`] (facade) · [`memberlist-proto`] (Sans-I/O core) ·
[`memberlist-reactor`] (tokio / smol driver) · [`memberlist-compio`] (compio driver) ·
[`memberlist-embedded`] (shared `no_std` core) · **`memberlist-smoltcp`** (this crate) ·
[`memberlist-embassy`] (embassy driver).

## License

`memberlist-smoltcp` is under the terms of the MPL-2.0 license.

See [LICENSE] for details.

Copyright (c) 2025 Al Liu.

Copyright (c) 2013 HashiCorp, Inc.

[smoltcp]: https://crates.io/crates/smoltcp
[`memberlist`]: https://crates.io/crates/memberlist
[`memberlist-proto`]: https://crates.io/crates/memberlist-proto
[`memberlist-reactor`]: https://crates.io/crates/memberlist-reactor
[`memberlist-compio`]: https://crates.io/crates/memberlist-compio
[`memberlist-embedded`]: https://crates.io/crates/memberlist-embedded
[`memberlist-embassy`]: https://crates.io/crates/memberlist-embassy
[LICENSE]: https://github.com/al8n/memberlist/blob/main/LICENSE
[Github-url]: https://github.com/al8n/memberlist/
[CI-url]: https://github.com/al8n/memberlist/actions/workflows/coverage.yml
[codecov-url]: https://app.codecov.io/gh/al8n/memberlist/
[doc-url]: https://docs.rs/memberlist-smoltcp
[crates-url]: https://crates.io/crates/memberlist-smoltcp
