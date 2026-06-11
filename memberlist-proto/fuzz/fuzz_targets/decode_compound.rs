#![no_main]

//! Fuzz the compound splitter: the `count`-then-`[len][part]*` framing must
//! reject a malformed count / truncated part without panicking or allocating an
//! attacker-declared multi-gigabyte parts vector.

use libfuzzer_sys::fuzz_target;
use memberlist_proto::decode_compound;

fuzz_target!(|data: &[u8]| {
  let _ = decode_compound(data);
});
