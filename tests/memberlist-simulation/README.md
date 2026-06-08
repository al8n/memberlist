<div align="center">
<h1>memberlist-simulation</h1>
</div>
<div align="center">

Deterministic single-threaded simulation harness for the Sans-I/O **memberlist** machine.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/memberlist-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/memberlist/coverage.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=mozilla" height="22">

</div>

## Introduction

`memberlist-simulation` drives [`memberlist-proto`] without sockets, threads, or a real
runtime: it feeds crafted packets and a virtual clock straight into the state machine, so
SWIM scenarios — gossip dissemination, direct and indirect probes, suspicion / refutation,
push/pull anti-entropy, reaping, QUIC datagrams, and the wire codec — run reproducibly and
deterministically.

It backs the protocol conformance suites and the VOPR-style deterministic adversarial
simulation. This is an internal testing crate for the [memberlist] workspace; it is not a
library you depend on to build a node — use the [`memberlist`] facade or a driver crate for
that.

## License

`memberlist-simulation` is under the terms of the MPL-2.0 license.

See [LICENSE] for details.

Copyright (c) 2025 Al Liu.

Copyright (c) 2013 HashiCorp, Inc.

[memberlist]: https://github.com/al8n/memberlist
[`memberlist`]: https://crates.io/crates/memberlist
[`memberlist-proto`]: https://crates.io/crates/memberlist-proto
[LICENSE]: https://github.com/al8n/memberlist/blob/main/LICENSE
[Github-url]: https://github.com/al8n/memberlist/
[CI-url]: https://github.com/al8n/memberlist/actions/workflows/coverage.yml
