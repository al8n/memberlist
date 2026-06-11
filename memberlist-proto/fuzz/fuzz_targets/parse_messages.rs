#![no_main]

//! Fuzz the compound/message parser over arbitrary plaintext: a crafted compound
//! count or part-length prefix must never panic, over-allocate, or hang.

use core::net::SocketAddr;

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use memberlist_proto::{parse_messages, typed::Message};
use smol_str::SmolStr;

fuzz_target!(|data: &[u8]| {
  let _: Result<Vec<Message<SmolStr, SocketAddr>>, _> =
    parse_messages(Bytes::copy_from_slice(data));
});
