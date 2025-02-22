# Releases

## 0.6.0

### Features

- Add `send_many` to let users send multiple packets through unreliable connection.
- Add `send_many_reliable` to let users send multiple packets through reliable connection.
- Redesign `Transport` trait, making it easier to implement for users.
- Rewriting encoding/decoding to support forward and backward compitibility.
- Support `zstd`, `brotli`, `lz4`, and `snappy` for compressing.
- Support `crc32`, `xxhash64`, `xxhash32`, `xxhash3`, `murmur3` for checksuming.
- Unify returned error, all exported APIs return `Error` on `Result::Err`.

### Breakage

- Remove `native-tls` supports
- Remove `s2n-quic` supports
- Remove `Wire` trait to simplify `Transport` trait
- Remove `JoinError`, add an new `Error::Multiple` variant

### Testing

- Add fuzzy testing for encoding/decoding
