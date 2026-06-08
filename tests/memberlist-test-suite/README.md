<div align="center">
<h1>memberlist-test-suite</h1>
</div>
<div align="center">

A driver-agnostic real-node test suite for the **memberlist** drivers.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/memberlist-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/memberlist/coverage.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=mozilla" height="22">

</div>

## Introduction

`memberlist-test-suite` is the shared, transport- and runtime-agnostic integration suite
that the async drivers ([`memberlist-compio`], [`memberlist-reactor`]) run against real
sockets. A driver implements a small `TestCluster` seam, and the suite drives the rest:
two-node join / leave, membership queries, directed reliable / unreliable I/O, the
compression / encryption / checksum transform matrix, and shutdown / teardown.

This is an internal testing crate (`publish = false`) for the [memberlist] workspace, not a
library you depend on directly.

## License

`memberlist-test-suite` is under the terms of the MPL-2.0 license.

See [LICENSE] for details.

Copyright (c) 2025 Al Liu.

Copyright (c) 2013 HashiCorp, Inc.

[memberlist]: https://github.com/al8n/memberlist
[`memberlist-compio`]: https://crates.io/crates/memberlist-compio
[`memberlist-reactor`]: https://crates.io/crates/memberlist-reactor
[LICENSE]: https://github.com/al8n/memberlist/blob/main/LICENSE
[Github-url]: https://github.com/al8n/memberlist/
[CI-url]: https://github.com/al8n/memberlist/actions/workflows/coverage.yml
