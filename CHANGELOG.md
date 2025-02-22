# Releases

## 0.6.0

### Features

- Redesign `Transport` trait, making it easier to implement for users.
- Rewriting encoding/decoding to support forward and backward compitibility.
- Support `zstd`, `brotli`, `lz4`, and `snappy` for compressing.
- Support `crc32`, `xxhash64`, `xxhash32`, `xxhash3`, `murmur3` for checksuming.

### Breakage

- Remove `native-tls` supports
- Remove `s2n-quic` supports
- Remove `Wire` trait to simplify `Transport` trait
