<div align="center">
<h1>memberlist-embassy</h1>
</div>
<div align="center">

Async `no_std` **SWIM** membership driver over the [embassy-net] network stack.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/memberlist-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/memberlist/coverage.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/memberlist?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-memberlist--embassy-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/memberlist-embassy?style=for-the-badge&logo=rust" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/memberlist-embassy?color=critical&logo=rust&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=mozilla" height="22">

</div>

## Introduction

`memberlist-embassy` drives the Sans-I/O core (through the shared [`memberlist-embedded`]
engine) over [embassy-net] on the [embassy] async runtime. It bridges the engine to
embassy-net's async-only `UdpSocket` / `TcpSocket` through a worker-mailbox `Runner` you
spawn as an embassy task; the `Memberlist` handle then drives membership from your own
tasks.

`no_std` + `alloc`. For an executor-free, caller-poll driver over the same engine, see
[`memberlist-smoltcp`]; for `std` hosts, see the [`memberlist`] facade.

## Installation

```toml
[dependencies]
memberlist-embassy = { version = "0.0.1", default-features = false, features = ["alloc"] }
```

## Example

Build the node and its `Runner` over your embassy-net stack, hand the `Runner` to the
embassy executor, then use the `Memberlist` handle:

```rust,ignore
use memberlist_embassy::{Memberlist, Runner};

#[embassy_executor::task]
async fn memberlist_driver(runner: Runner</* … */>) {
    runner.run().await; // owns the embassy-net sockets; pumps the engine forever
}

// In your network setup task, after embassy-net's `Stack` is up:
let (node, runner) = Memberlist::new(/* options, endpoint options, embassy-net stack */);
spawner.must_spawn(memberlist_driver(runner));

node.join(/* seeds */).await?;
// … node.members(), node.send(...), node.leave().await, …
```

A complete, runnable wiring (two nodes joining on an emulated Cortex-M) lives in the
`memberlist-embassy-qemu` execution proof in the repository.

## Feature flags

| Feature | Description |
|---------|-------------|
| `std` *(default)* | host builds and the test harness |
| `alloc` | `no_std` with a global allocator (bare metal) — build with `--no-default-features --features alloc` |
| `lz4` / `-snappy` / `-zstd` / `-brotli` | gossip-plane compression |
| `aes-gcm` / `-chacha20-poly1305` | gossip-plane AEAD encryption |
| `crc32` / `-xxhash64` / … | gossip-plane checksum |
| `cidr` | IP allow-list admission |

## Design

- Built on the transport-agnostic [`memberlist-embedded`] `Engine`, shared with
  [`memberlist-smoltcp`].
- A worker-mailbox `Runner` owns embassy-net's async-only `UdpSocket` / `TcpSocket` and
  pumps the engine; the public `Memberlist` handle communicates with it over a mailbox, so
  membership calls never block the socket worker.

## The memberlist family

[`memberlist`] (facade) · [`memberlist-proto`] (Sans-I/O core) ·
[`memberlist-reactor`] (tokio / smol driver) · [`memberlist-compio`] (compio driver) ·
[`memberlist-embedded`] (shared `no_std` core) · [`memberlist-smoltcp`] (smoltcp driver) ·
**`memberlist-embassy`** (this crate).

## License

`memberlist-embassy` is under the terms of the MPL-2.0 license.

See [LICENSE] for details.

Copyright (c) 2025 Al Liu.

Copyright (c) 2013 HashiCorp, Inc.

[embassy]: https://embassy.dev
[embassy-net]: https://crates.io/crates/embassy-net
[`memberlist`]: https://crates.io/crates/memberlist
[`memberlist-proto`]: https://crates.io/crates/memberlist-proto
[`memberlist-reactor`]: https://crates.io/crates/memberlist-reactor
[`memberlist-compio`]: https://crates.io/crates/memberlist-compio
[`memberlist-embedded`]: https://crates.io/crates/memberlist-embedded
[`memberlist-smoltcp`]: https://crates.io/crates/memberlist-smoltcp
[LICENSE]: https://github.com/al8n/memberlist/blob/main/LICENSE
[Github-url]: https://github.com/al8n/memberlist/
[CI-url]: https://github.com/al8n/memberlist/actions/workflows/coverage.yml
[codecov-url]: https://app.codecov.io/gh/al8n/memberlist/
[doc-url]: https://docs.rs/memberlist-embassy
[crates-url]: https://crates.io/crates/memberlist-embassy
