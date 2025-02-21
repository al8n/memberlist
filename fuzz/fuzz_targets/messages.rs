#![no_main]

use libfuzzer_sys::fuzz_target;
use memberlist_proto::{Data, Message};

fuzz_target!(|data: Message<String, Vec<u8>>| {
  let encoded_len = data.encoded_len();
  let mut buf = vec![0; encoded_len * 2];
  let written = match data.encode(&mut buf) {
    Ok(written) => written,
    Err(e) => {
      panic!("Encode Error: {}", e);
    }
  };

  match Message::<String, Vec<u8>>::decode(&buf[..written]) {
    Ok((readed, decoded)) => {
      assert!(data == decoded && written == readed && encoded_len == written)
    }
    Err(e) => {
      panic!("Decode Error: {}", e);
    }
  }
});
