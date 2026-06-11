#![no_main]

//! Fuzz the inbound label-strip + frame decode: an arbitrary `[label?][frame]`
//! must be rejected cleanly. The first byte selects whether a cluster label is
//! expected, exercising both the labeled and unlabeled strip paths.

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use memberlist_proto::{DecodeOptions, decode_incoming};

fuzz_target!(|data: &[u8]| {
  let (label, rest) = match data.split_first() {
    Some((selector, rest)) if selector & 1 == 1 => {
      (Some(Bytes::from_static(b"fuzz-cluster")), rest)
    }
    Some((_, rest)) => (None, rest),
    None => (None, data),
  };
  let _ = decode_incoming(Bytes::copy_from_slice(rest), &DecodeOptions::new(label));
});
