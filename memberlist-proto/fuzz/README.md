# memberlist-proto fuzz targets

Fuzz harnesses for the untrusted-input parsers — the decode surfaces a single
inbound datagram reaches before any authentication. Each target feeds arbitrary
bytes to one parser and discards the result; libFuzzer asserts no panic, abort,
or unbounded allocation.

| Target | Surface |
|---|---|
| `parse_messages` | the compound / single-message parser (count + part-length prefixes) |
| `decode_compound` | the compound splitter (count-vs-remaining bound before allocation) |
| `decode_incoming` | inbound cluster-label strip + frame decode (labeled and unlabeled) |
| `unwrap_transforms` | the tag-driven checksum / decompress / decrypt unwrap loop (decompression bombs, non-canonical nesting) |

## Running

Requires the nightly toolchain and `cargo-fuzz` (`cargo install cargo-fuzz`):

```sh
cargo +nightly fuzz run parse_messages
cargo +nightly fuzz run unwrap_transforms -- -max_total_time=300
```

This crate is its own workspace (it needs the sanitizer runtime), so it is built
only through `cargo fuzz`, never the host `--workspace` build.
