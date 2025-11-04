<div align="center">

<img src="https://raw.githubusercontent.com/al8n/memberlist/main/art/logo.png" height = "200px">

<h1>Memberlist</h1>


</div>
<div align="center">

A highly customable, adaptable, runtime agnostic and WASM/WASI friendly Gossip protocol which helps manage cluster membership and member failure detection.

Port and improve [HashiCorp's memberlist](https://github.com/hashicorp/memberlist) to Rust.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/memberlist-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
<img alt="LoC" src="https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fal8n%2Fd29ceff54c025fe4e8b144a51efb9324%2Fraw%2Fmemberlist" height="22">
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/memberlist/coverage.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/memberlist?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-memberlist-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/memberlist?style=for-the-badge&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMiA1MTIiIHhtbDpzcGFjZT0icHJlc2VydmUiPg0KPGc+DQoJPGc+DQoJCTxwYXRoIGQ9Ik0yNTYsMEwzMS41MjgsMTEyLjIzNnYyODcuNTI4TDI1Niw1MTJsMjI0LjQ3Mi0xMTIuMjM2VjExMi4yMzZMMjU2LDB6IE0yMzQuMjc3LDQ1Mi41NjRMNzQuOTc0LDM3Mi45MTNWMTYwLjgxDQoJCQlsMTU5LjMwMyw3OS42NTFWNDUyLjU2NHogTTEwMS44MjYsMTI1LjY2MkwyNTYsNDguNTc2bDE1NC4xNzQsNzcuMDg3TDI1NiwyMDIuNzQ5TDEwMS44MjYsMTI1LjY2MnogTTQzNy4wMjYsMzcyLjkxMw0KCQkJbC0xNTkuMzAzLDc5LjY1MVYyNDAuNDYxbDE1OS4zMDMtNzkuNjUxVjM3Mi45MTN6IiBmaWxsPSIjRkZGIi8+DQoJPC9nPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPC9zdmc+DQo=" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/memberlist?color=critical&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBzdGFuZGFsb25lPSJubyI/PjwhRE9DVFlQRSBzdmcgUFVCTElDICItLy9XM0MvL0RURCBTVkcgMS4xLy9FTiIgImh0dHA6Ly93d3cudzMub3JnL0dyYXBoaWNzL1NWRy8xLjEvRFREL3N2ZzExLmR0ZCI+PHN2ZyB0PSIxNjQ1MTE3MzMyOTU5IiBjbGFzcz0iaWNvbiIgdmlld0JveD0iMCAwIDEwMjQgMTAyNCIgdmVyc2lvbj0iMS4xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHAtaWQ9IjM0MjEiIGRhdGEtc3BtLWFuY2hvci1pZD0iYTMxM3guNzc4MTA2OS4wLmkzIiB3aWR0aD0iNDgiIGhlaWdodD0iNDgiIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIj48ZGVmcz48c3R5bGUgdHlwZT0idGV4dC9jc3MiPjwvc3R5bGU+PC9kZWZzPjxwYXRoIGQ9Ik00NjkuMzEyIDU3MC4yNHYtMjU2aDg1LjM3NnYyNTZoMTI4TDUxMiA3NTYuMjg4IDM0MS4zMTIgNTcwLjI0aDEyOHpNMTAyNCA2NDAuMTI4QzEwMjQgNzgyLjkxMiA5MTkuODcyIDg5NiA3ODcuNjQ4IDg5NmgtNTEyQzEyMy45MDQgODk2IDAgNzYxLjYgMCA1OTcuNTA0IDAgNDUxLjk2OCA5NC42NTYgMzMxLjUyIDIyNi40MzIgMzAyLjk3NiAyODQuMTYgMTk1LjQ1NiAzOTEuODA4IDEyOCA1MTIgMTI4YzE1Mi4zMiAwIDI4Mi4xMTIgMTA4LjQxNiAzMjMuMzkyIDI2MS4xMkM5NDEuODg4IDQxMy40NCAxMDI0IDUxOS4wNCAxMDI0IDY0MC4xOTJ6IG0tMjU5LjItMjA1LjMxMmMtMjQuNDQ4LTEyOS4wMjQtMTI4Ljg5Ni0yMjIuNzItMjUyLjgtMjIyLjcyLTk3LjI4IDAtMTgzLjA0IDU3LjM0NC0yMjQuNjQgMTQ3LjQ1NmwtOS4yOCAyMC4yMjQtMjAuOTI4IDIuOTQ0Yy0xMDMuMzYgMTQuNC0xNzguMzY4IDEwNC4zMi0xNzguMzY4IDIxNC43MiAwIDExNy45NTIgODguODMyIDIxNC40IDE5Ni45MjggMjE0LjRoNTEyYzg4LjMyIDAgMTU3LjUwNC03NS4xMzYgMTU3LjUwNC0xNzEuNzEyIDAtODguMDY0LTY1LjkyLTE2NC45MjgtMTQ0Ljk2LTE3MS43NzZsLTI5LjUwNC0yLjU2LTUuODg4LTMwLjk3NnoiIGZpbGw9IiNmZmZmZmYiIHAtaWQ9IjM0MjIiIGRhdGEtc3BtLWFuY2hvci1pZD0iYTMxM3guNzc4MTA2OS4wLmkwIiBjbGFzcz0iIj48L3BhdGg+PC9zdmc+&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPHN2ZyBpZD0iX+WbvuWxgl8xIiBkYXRhLW5hbWU9IuWbvuWxgiAxIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA0NzMuNDcgMjU1LjEyMiI+CiAgPGRlZnM+CiAgICA8c3R5bGU+CiAgICAgIC5jbHMtMSB7CiAgICAgICAgZmlsbDogI2ZmZjsKICAgICAgICBzdHJva2Utd2lkdGg6IDBweDsKICAgICAgfQogICAgPC9zdHlsZT4KICA8L2RlZnM+CiAgPHBvbHlnb24gY2xhc3M9ImNscy0xIiBwb2ludHM9IjM0MC4wNjUgLjQ4NCAzMzQuMzczIDEuMjA5IDMyOC45MjkgMi4xNzUgMzIzLjczMSAzLjYyOCAzMTguNzggNS4zMTggMzEzLjgzMiA3LjAxMiAzMDkuMTI3IDkuMTkgMzA0LjY3MiAxMS42MDYgMzAwLjQ2NiAxNC4yNjggMjk2LjI1OSAxNy4xNjkgMjkyLjI5NyAyMC4zMTIgMjg4LjU4NiAyMy42OTcgMjg1LjEyIDI3LjMyNiAyODEuOTAyIDMxLjE5NCAyNzguNjg0IDM1LjMwNyAyNzUuOTYyIDM5LjQxNiAyNzMuMjQgNDQuMDExIDI3MC43NjUgNDguNjA3IDI2OC41MzkgNTMuNjg1IDI2Ni41NTkgNDguMzYzIDI2NC4wODMgNDMuMjg1IDI2MS42MDggMzguNDUxIDI1OC42MzkgMzMuODU0IDI1NS40MjIgMjkuNzQ0IDI1MS45NTUgMjUuODc1IDI0OC4yNDQgMjIuMjQ3IDI0NC41MzEgMTguODYzIDI0MC4zMjIgMTUuNzE5IDIzNi4xMTYgMTMuMDU5IDIzMS40MTQgMTAuMzk3IDIyNi45NTkgOC4yMjIgMjIyLjAwOCA2LjI4OCAyMTcuMDU3IDQuNTk0IDIxMi4xMDkgMy4xNDMgMjA2LjkxMSAxLjkzNCAyMDEuNDY0IC45NjggMTkwLjU3NiAwIDE3OS45MzIgMCAxNzQuNzM1IC40ODQgMTY5Ljc4NiAuOTY4IDE2NC44MzUgMS42OTMgMTYwLjEzNCAyLjkwMyAxNTUuNjc4IDQuMTEyIDE1MS4yMjMgNS41NjIgMTQ2Ljc2NyA3LjI1MyAxNDIuODA3IDkuMTkgMTM4LjYwMiAxMS4xMjUgMTM0Ljg4OCAxMy41NDEgMTMxLjE3NSAxNS45NiAxMjcuNzExIDE4LjYxOSAxMjQuMjQ0IDIxLjUyMiAxMjEuMDI5IDI0LjY2NiAxMTguMDU4IDI3LjgwOSAxMTUuMDg3IDMxLjE5NCAxMTIuMzY1IDM0LjgyMiAxMDkuODg5IDM4LjY5MSAxMDcuNjYzIDQyLjU2IDEwNy42NjMgNS4wNzggMCA1LjA3OCAwIDU4Ljc2NCAzMy45MDcgNTguNzY0IDMzLjkwNyAyMDAuNDcgMCAyMDAuNDcgMCAyNTUuMTIyIDE1Ni42NjcgMjU1LjEyMiAxNTYuNjY3IDIwMC40NyAxMDcuNjYzIDIwMC40NyAxMDcuNjYzIDEwOC4zMzcgMTA3LjkwOSAxMDMuMjU5IDEwOC42NTIgOTguNDIxIDEwOS4zOTYgOTMuODI3IDExMC4zODUgODkuNDc0IDExMS42MjMgODUuMTIxIDExMy4xMDcgODEuMjUyIDExNC44NCA3Ny4zODMgMTE2LjgyIDczLjk5OCAxMTkuMDQ3IDcwLjYxMSAxMjEuNzcyIDY3LjcxMSAxMjQuNDk0IDY1LjA1MSAxMjcuNDYxIDYyLjYzMyAxMzAuOTI5IDYwLjQ1NSAxMzQuNjM5IDU4LjUyIDEzOC4zNTIgNTcuMDcgMTQyLjU2MSA1NS44NiAxNDcuMjYzIDU0Ljg5NSAxNTEuOTY1IDU0LjQxIDE1Ny4xNjIgNTQuMTY3IDE2MS4zNzEgNTQuMTY3IDE2NS4zMzEgNTQuNjUxIDE2OS4yOTEgNTUuMzc2IDE3Mi43NTUgNTYuMzQ1IDE3Ni4yMjEgNTcuNTU0IDE3OS40MzkgNTkuMDA1IDE4Mi40MDggNjAuNjk4IDE4NS4xMyA2Mi44NzMgMTg3LjYwNSA2NS4yOTIgMTkwLjA4MSA2Ny45NTIgMTkyLjA2MSA3MC44NTQgMTk0LjA0MSA3NC4yMzkgMTk1LjUyNCA3OC4xMDggMTk3LjAxMSA4MS45NzcgMTk4LjI0OSA4Ni41NzQgMTk5LjIzOCA5MS4xNjcgMTk5Ljk4IDk2LjI0NiAyMDAuNzIyIDEwMS44MDkgMjAwLjk3MSAxMDcuODUyIDIwMC45NzEgMjU1LjEyMiAzMDcuMzk3IDI1NS4xMjIgMzA3LjM5NyAyMDAuNDcgMjczLjQ4NyAyMDAuNDcgMjczLjQ4NyAxMTMuNDE1IDI3My43MzYgMTA4LjMzNyAyNzMuOTgzIDEwMy4yNTkgMjc0LjQ3OCA5OC40MjEgMjc1LjQ2NiA5My44MjcgMjc2LjQ1OCA4OS40NzQgMjc3LjY5NiA4NS4xMjEgMjc5LjE4IDgxLjI1MiAyODAuOTEzIDc3LjM4MyAyODIuODk0IDczLjk5OCAyODUuMTIgNzAuNjExIDI4Ny41OTUgNjcuNzExIDI5MC41NjcgNjUuMDUxIDI5My41MzQgNjIuNjMzIDI5Ny4wMDIgNjAuNDU1IDMwMC40NjYgNTguNTIgMzA0LjQyNSA1Ny4wNyAzMDguNjM1IDU1Ljg2IDMxMy4zMzYgNTQuODk1IDMxOC4wMzggNTQuNDEgMzIzLjIzNSA1NC4xNjcgMzI3LjQ0NCA1NC4xNjcgMzMxLjQwNCA1NC42NTEgMzM1LjM2NCA1NS4zNzYgMzM4LjgyOCA1Ni4zNDUgMzQyLjI5MiA1Ny41NTQgMzQ1LjUwOSA1OS4wMDUgMzQ4LjQ4MSA2MC42OTggMzUxLjIwMyA2Mi44NzMgMzUzLjY3OCA2NS4yOTIgMzU1LjkwNCA2Ny45NTIgMzU4LjEzMyA3MC44NTQgMzYwLjExNCA3NC4yMzkgMzYzLjA4MiA4MS45NzcgMzY0LjMyIDg2LjU3NCAzNjUuMzExIDkxLjE2NyAzNjYuMDUzIDk2LjI0NiAzNjYuNzk1IDEwMS44MDkgMzY3LjA0NCAxMDcuODUyIDM2Ny4wNDQgMjU1LjEyMiA0NzMuNDcgMjU1LjEyMiA0NzMuNDcgMjAwLjQ3IDQzOS41NiAyMDAuNDcgNDM5LjU2IDg2LjMzIDQzOS4zMTMgNzcuNjI0IDQzOC4zMjIgNjkuNjQ1IDQzNi44MzggNjEuOTA3IDQzNC44NTggNTQuNjUxIDQzMi4zODMgNDcuODgyIDQyOS40MTQgNDEuNTk1IDQyNS45NDggMzUuNzg4IDQyMS45ODggMzAuMjI5IDQxNy41MzIgMjUuMzkxIDQxMy4wNzcgMjAuNzk3IDQwNy44OCAxNi45MjggNDAyLjY4MiAxMy4zIDM5Ni45OTEgMTAuMTU3IDM5MS4wNTIgNy4yNTMgMzg0Ljg2MyA1LjA3OCAzNzguNjc0IDMuMTQzIDM3MS45OTMgMS42OTMgMzY1LjU1OCAuNzI1IDM1OC42MjkgMCAzNDUuNzU5IDAgMzQwLjA2NSAuNDg0Ii8+Cjwvc3ZnPg==" height="22">

[<img alt="github" src="https://img.shields.io/discord/835936528140206122?style=for-the-badge&logo=discord&logoColor=white&label=Discord&color=7289da" height="22">][discord]

English | [简体中文][zh-cn-url]

</div>

## Introduction

memberlist is a rust crate that manages cluster membership and member failure detection using a gossip based protocol.

The use cases for such a library are far-reaching: all distributed systems require membership, and memberlist is a re-usable solution to managing cluster membership and node failure detection.

memberlist is eventually consistent but converges quickly on average. The speed at which it converges can be heavily tuned via various knobs on the protocol. Node failures are detected and network partitions are partially tolerated by attempting to communicate to potentially dead nodes through multiple routes.

memberlist is WASM/WASI friendly, all crates can be compiled to `wasm-wasi` and `wasm-unknown-unknown` (need to configure the crate features).

## Installation

- By using `TCP/UDP`, `TLS/UDP` transport

  ```toml
  memberlist = { version = "0.6", features = [
    "tcp",
    # Enable a checksum, as UDP is not reliable.
    # Built in supports are: "crc32", "xxhash64", "xxhash32", "xxhash3", "murmur3"
    "crc32",
    # Enable a compression, this is optional,
    # and possible values are `snappy`, `brotli`, `zstd` and `lz4`.
    # You can enable all.
    "snappy",
    # Enable encryption, this is optional,
    "encryption",
    # Enable a async runtime
    # Builtin supports are `tokio`, `smol`
    "tokio",
    # Enable one tls implementation. This is optional.
    # Users can just use encryption feature with plain TCP.
    #
    # "tls",
  ] }
  ```

- By using `QUIC/QUIC` transport

  For `QUIC/QUIC` transport, as QUIC is secure and reliable, so enable checksum or encryption makes no sense.

  ```toml
  memberlist = { version = "0.6", features = [
    # Enable a compression, this is optional,
    # and possible values are `snappy`, `brotli`, `zstd` and `lz4`.
    # You can enable all.
    "snappy",
    # Enable a async runtime
    # Builtin supports are `tokio`, `smol`
    "tokio",
    # Enable one of the QUIC implementation
    # Builtin support is `quinn`
    "quinn",
  ] }
  ```

## Examples

- [serf](https://github.com/al8n/serf): A decentralized solution for service discovery and orchestration that is lightweight, highly available, and fault tolerant.
- [examples/toydb](https://github.com/al8n/memberlist/tree/main/examples/toydb): a toy eventually consistent distributed key-value database.

## Protocol

memberlist is based on ["SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol"](http://ieeexplore.ieee.org/document/1028914/). However, Hashicorp developers extends the protocol in a number of ways:

Several extensions are made to increase propagation speed and convergence rate.
Another set of extensions, that Hashicorp developers call Lifeguard, are made to make memberlist more robust in the presence of slow message processing (due to factors such as CPU starvation, and network delay or loss).
For details on all of these extensions, please read Hashicorp's paper ["Lifeguard : SWIM-ing with Situational Awareness"](https://arxiv.org/abs/1707.00788), along with the memberlist source.

## Design

Unlike the original Go implementation, Rust's memberlist use highly generic and layered architecture, users can easily implement a component by themselves and plug it to the memberlist. Users can even custom their own `Id` and `Address`.

Here are the layers:

- **Transport Layer**

  By default, Rust's memberlist provides two kinds of transport -- [`QuicTransport`](https://docs.rs/memberlist-quic/struct.QuicTransport.html) and [`NetTransport`](https://docs.rs/memberlist-net/struct.NetTransport.html).

  - **Runtime Layer**

    Async runtime agnostic are provided by [`agnostic`'s Runtime](https://docs.rs/agnostic/trait.Runtime.html) trait, `tokio`, and `smol` are supported by default. Users can implement their own [`Runtime`](https://docs.rs/agnostic/trait.Runtime.html) and plug it into the memberlist.

  - **Address Resolver Layer**

    The address resolver layer is supported by [`nodecraft`'s AddressResolver](https://docs.rs/nodecraft/latest/nodecraft/resolver/trait.AddressResolver.html) trait.

  - **[`NetTransport`](https://docs.rs/memberlist-net/struct.NetTransport.html)**

    Builtin stream layers for `NetTransport`:

    - [`Tcp`](https://docs.rs/memberlist-net/stream_layer/tcp/struct.Tcp.html): based on TCP and UDP
    - [`Tls`](https://docs.rs/memberlist-net/stream_layer/tls/struct.Tls.html): based on [`rustls`](https://docs.rs/rustls) and UDP

  - **[`QuicTransport`](https://docs.rs/memberlist-quic/struct.QuicTransport.html)**

    QUIC transport is an experimental transport implementation, it is well tested but still experimental.

    Builtin stream layers for `QuicTransport`:
    - [`Quinn`](https://docs.rs/memberlist-quic/stream_layer/quinn/struct.Quinn.html): based on [`quinn`](https://docs.rs/quinn)

  Users can still implement their own stream layer for different kinds of transport implementations.

- **Delegate Layer**
  
  This layer is used as a reactor for different kinds of messages.

  - **`Delegate`**
  
    Delegate is the trait that clients must implement if they want to hook into the gossip layer of Memberlist. All the methods must be thread-safe, as they can and generally will be called concurrently.

    Here are the sub delegate traits:

    - **`AliveDelegate`**

      Used to involve a client in processing a node "alive" message. When a node joins, either through a packet gossip or promised push/pull, we update the state of that node via an alive message. This can be used to filter a node out and prevent it from being considered a peer using application specific logic.

    - **`ConflictDelegate`**

      Used to inform a client that a node has attempted to join which would result in a name conflict. This happens if two clients are configured with the same name but different addresses.

    - **`EventDelegate`**

      A simpler delegate that is used only to receive notifications about members joining and leaving. The methods in this delegate may be called by multiple threads, but never concurrently. This allows you to reason about ordering.

    - **`MergeDelegate`**

      Used to involve a client in a potential cluster merge operation. Namely, when a node does a promised push/pull (as part of a join), the delegate is involved and allowed to cancel the join based on custom logic. The merge delegate is NOT invoked as part of the push-pull anti-entropy.

    - **`NodeDelegate`**

      Used to manage node related events. e.g. metadata

    - **`PingDelegate`**

      Used to notify an observer how long it took for a ping message to complete a round trip. It can also be used for writing arbitrary byte slices into ack messages. Note that in order to be meaningful for RTT estimates, this delegate does not apply to indirect pings, nor fallback pings sent over promised connection.

  - **`CompositeDelegate`**

    CompositeDelegate is a helpful struct to split the `Delegate` into multiple small delegates, so that users do not need to implement full `Delegate` when they only want to custom some methods in the Delegate.

## Q & A

- ***Does Rust's memberlist implemenetation compatible to Go's memberlist?***

  No but yes! Rust's memberlist impelementation use a protobuf-like forward and backward compatible encoding/decoding, Go's implementation
  use message pack. It is possible in theory, users need to implement their own transport layer, but not recommand, you are facing
  expensive overhead!

- ***If Go's memberlist adds more functionalities, will this project also support?***
  
  Yes! And this project may also add more functionalities whereas the Go's memberlist does not have. e.g. wasmer support, bindings to other languages and etc.

## Related Projects

- [`agnostic`](https://github.com/al8n/agnostic): helps you to develop runtime agnostic crates
- [`getifs`](https://github.com/al8n/getifs): A bunch of cross platform network tools for fetching interfaces, multicast addresses, local ip addresses, private ip addresses, public ip addresses and etc.
- [`nodecraft`](https://github.com/al8n/nodecraft): crafting seamless node operations for distributed systems, which provides foundational traits for node identification and address resolution.
- [`peekable`](https://github.com/al8n/peekable): peekable reader and async reader

### Which crates are using memberlist?

- [`serf`](https://github.com/al8n/serf): A decentralized solution for service discovery and orchestration that is lightweight, highly available, and fault tolerant.

#### License

`memberlist` is under the terms of the MPL-2.0 license.

See [LICENSE](./LICENSE) for details.

Copyright (c) 2025 Al Liu.

Copyright (c) 2013 HashiCorp, Inc.

[Github-url]: https://github.com/al8n/memberlist/
[CI-url]: https://github.com/al8n/memberlist/actions/workflows/ci.yml
[doc-url]: https://docs.rs/memberlist
[crates-url]: https://crates.io/crates/memberlist
[codecov-url]: https://app.codecov.io/gh/al8n/memberlist/
[zh-cn-url]: https://github.com/al8n/memberlist/tree/main/README-zh_CN.md
[discord]: https://discord.gg/f3EtWBM7ke
