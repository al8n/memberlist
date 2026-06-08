<div align="center">
<h1>memberlist-embassy-qemu</h1>
</div>
<div align="center">

Bare-metal QEMU execution proof for the **memberlist** embassy driver.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/memberlist-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=mozilla" height="22">

</div>

## Introduction

`memberlist-embassy-qemu` boots [`memberlist-embassy`] on an emulated Cortex-M (the
`mps2-an386` machine) under QEMU and joins two nodes over embassy-net — an end-to-end proof
that the `no_std` async driver runs on real bare metal, with its own `cortex-m-rt` runtime,
a SysTick embassy-time driver, and a no_std entropy backend.

It is the CI execution gate for the embassy driver and doubles as a complete, runnable
wiring example. Because it pins a foreign default target, it is **excluded** from the host
workspace and built on its own:

```sh
cd tests/memberlist-embassy-qemu && cargo run
```

This is an internal proof binary (`publish = false`) for the [memberlist] workspace.

## License

`memberlist-embassy-qemu` is under the terms of the MPL-2.0 license.

See [LICENSE] for details.

Copyright (c) 2025 Al Liu.

Copyright (c) 2013 HashiCorp, Inc.

[memberlist]: https://github.com/al8n/memberlist
[`memberlist-embassy`]: https://crates.io/crates/memberlist-embassy
[LICENSE]: https://github.com/al8n/memberlist/blob/main/LICENSE
[Github-url]: https://github.com/al8n/memberlist/
