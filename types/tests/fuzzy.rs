use bytes::{Bytes, BytesMut};
use core::{
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
  time::Duration,
};
use memberlist_types::{
  Ack, Alive, ChecksumAlgorithm, CompressAlgorithm, Data, Dead, EncryptionAlgorithm, ErrorResponse,
  IndirectPing, Label, Message, MessagesDecoder, MessagesDecoderIter, Nack, Ping, ProtoDecoder,
  ProtoDecoderError, ProtoEncoder, ProtoEncoderError, PushNodeState, PushPull, SecretKey, Suspect,
};
use nodecraft::{Domain, HostAddr, Node, NodeId};

fn fuzzy<D>(data: D) -> bool
where
  D: Data + Eq,
{
  let encoded_len = data.encoded_len();
  let mut buf = vec![0; encoded_len * 2];
  let written = match data.encode(&mut buf) {
    Ok(written) => written,
    Err(e) => {
      println!("Encode Error: {}", e);
      return false;
    }
  };

  match D::decode(&buf[..written]) {
    Ok((readed, decoded)) => data == decoded && written == readed && encoded_len == written,
    Err(e) => {
      println!("Decode Error: {}", e);
      false
    }
  }
}

macro_rules! quickcheck {
  ($($ty:ident), +$(,)?) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< $ty:snake _fuzzy >](val: $ty) -> bool {
          fuzzy(val)
        }
      )*
    }
  };
  (@ $($ty:ident),  +$(,)?) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< $ty:snake _fuzzy >](val: $ty) -> bool {
          quickcheck!(@inner $ty = val)
        }
      )+
    }
  };
  (@<T> $($ty:ident[ $($inner:ident), +$(,)? ]),+$(,)?) => {
    paste::paste! {
      $(
        $(
          #[quickcheck_macros::quickcheck]
          fn [< $ty:snake _ $inner:snake _fuzzy >](val: $ty<$inner>) -> bool {
            quickcheck!(@inner $ty<$inner> = val)
          }
        )*
      )+
    }
  };
  (@<I, A> $($ty:ident[ $(($id:ident, $addr: ident)), +$(,)? ]),+$(,)?) => {
    paste::paste! {
      $(
        $(
          #[quickcheck_macros::quickcheck]
          fn [< $ty:snake _ $id:snake _ $addr:snake _fuzzy >](val: $ty<$id, $addr>) -> bool {
            quickcheck!(@inner $ty<$id, $addr> = val)
          }
        )*
      )+
    }
  };
  (@inner $ty:ty = $val:ident) => {{
    let encoded_len = $val.encoded_len();
    let mut buf = vec![0; $val.encoded_len() * 2];
    let written = match $val.encode(&mut buf) {
      Ok(len) => len,
      Err(e) => {
        println!("Encode Error {}", e);
        return false;
      },
    };

    let (readed, decoded) = match <$ty>::decode(&buf[..written]) {
      Ok((readed, decoded)) => (readed, decoded),
      Err(e) => {
        println!("Decode Error {}", e);
        return false;
      },
    };

    $val == decoded && written == readed && encoded_len == written
  }};
}

type VecBytes = Vec<u8>;

quickcheck!(
  u8,
  u16,
  u32,
  u64,
  u128,
  i8,
  i16,
  i32,
  i64,
  i128,
  char,
  bool,
  IpAddr,
  Ipv4Addr,
  Ipv6Addr,
  SocketAddrV4,
  Duration,
  NodeId,
  Domain,
  HostAddr,
  String,
  VecBytes,
  Label,
);

quickcheck!(
  @<T>
  Suspect[
    u8,
    u16,
    u32,
    u64,
    u128,
    i8,
    i16,
    i32,
    i64,
    i128,
    char,
    bool,
    IpAddr,
    Ipv4Addr,
    Ipv6Addr,
    SocketAddrV4,
    Duration,
    NodeId,
    Domain,
    HostAddr,
    String,
    VecBytes,
  ],
  Dead[
    u8,
    u16,
    u32,
    u64,
    u128,
    i8,
    i16,
    i32,
    i64,
    i128,
    char,
    bool,
    IpAddr,
    Ipv4Addr,
    Ipv6Addr,
    SocketAddrV4,
    Duration,
    NodeId,
    Domain,
    HostAddr,
    String,
    VecBytes,
  ]
);

type Messages<I, A> = Message<I, A>;

quickcheck!(
  @<I, A>
  Node[
    (u32, SocketAddrV4),
    (u32, String),
    (IpAddr, SocketAddrV4),
    (IpAddr, String),
    (String, String),
    (String, SocketAddrV4),
  ],
  Alive[(u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)],
  Ping[(u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)],
  IndirectPing[(u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)],
  PushNodeState[(u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)],
  PushPull[(u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)],
  Message[(u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)],
  Messages[(u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)],
);

quickcheck!(
  @
  Ack,
  Nack,
  ErrorResponse,
);

#[quickcheck_macros::quickcheck]
fn socket_addr_v6_fuzzy(value: SocketAddrV6) -> bool {
  let mut buf = [0; 32];
  let len = Data::encoded_len(&value);
  let Ok(encoded_len) = Data::encode(&value, &mut buf[..len]) else {
    return false;
  };
  let (bytes_read, decoded) = match SocketAddrV6::decode(&buf[..encoded_len]) {
    Ok((bytes_read, decoded)) => (bytes_read, decoded),
    Err(e) => {
      println!("error: {}", e);
      return false;
    }
  };
  value.ip() == decoded.ip()
    && value.port() == decoded.port()
    && len == encoded_len
    && len == bytes_read
}

#[quickcheck_macros::quickcheck]
fn socket_addr_fuzzy(value: SocketAddr) -> bool {
  let mut buf = [0; 32];
  let len = Data::encoded_len(&value);
  let Ok(encoded_len) = Data::encode(&value, &mut buf[..len]) else {
    return false;
  };
  let (bytes_read, decoded) = match SocketAddr::decode(&buf[..encoded_len]) {
    Ok((bytes_read, decoded)) => (bytes_read, decoded),
    Err(e) => {
      println!("error: {}", e);
      return false;
    }
  };
  value.ip() == decoded.ip()
    && value.port() == decoded.port()
    && len == encoded_len
    && len == bytes_read
}

#[quickcheck_macros::quickcheck]
fn f32_fuzzy(value: f32) -> bool {
  let mut buf = [0; 32];
  let len = Data::encoded_len(&value);
  let Ok(encoded_len) = Data::encode(&value, &mut buf[..len]) else {
    return false;
  };
  let (bytes_read, decoded) = match f32::decode(&buf[..encoded_len]) {
    Ok((bytes_read, decoded)) => (bytes_read, decoded),
    Err(e) => {
      println!("error: {}", e);
      return false;
    }
  };
  if value.is_nan() {
    decoded.is_nan()
  } else {
    value == decoded && len == encoded_len && len == bytes_read
  }
}

#[quickcheck_macros::quickcheck]
fn f64_fuzzy(value: f64) -> bool {
  let mut buf = [0; 32];
  let len = Data::encoded_len(&value);
  let Ok(encoded_len) = Data::encode(&value, &mut buf[..len]) else {
    return false;
  };
  let (bytes_read, decoded) = match f64::decode(&buf[..encoded_len]) {
    Ok((bytes_read, decoded)) => (bytes_read, decoded),
    Err(e) => {
      println!("error: {}", e);
      return false;
    }
  };
  if value.is_nan() {
    decoded.is_nan()
  } else {
    value == decoded && len == encoded_len && len == bytes_read
  }
}

#[quickcheck_macros::quickcheck]
fn secret_key_fuzzy(_: SecretKey) -> bool {
  true
}

macro_rules! proto_encoder_decoder_unit_test {
  (@single<I, A> $($name:ident($encoder:expr, $label:expr)[ $(($id:ident, $addr: ident)), +$(,)? ]),+$(,)?) => {
    paste::paste! {
      $(
        $(
          #[quickcheck_macros::quickcheck]
          fn [< proto_ $name:snake _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
            let res: Result<(), Box<dyn std::error::Error>> = futures::executor::block_on(async move {
              let mut encoder = $encoder;
              let label = $label;
              encoder.with_label(&label);

              let messages = [message];
              encoder.with_messages(&messages);
              let data = encoder.encode().collect::<Result<Vec<_>, ProtoEncoderError>>()?;

              let mut msgs = Vec::new();
              let mut decoder = ProtoDecoder::default();
              decoder.with_label(label);

              for payload in data {
                let data = decoder.decode(BytesMut::from(Bytes::from(payload))).await?;

                let decoder = MessagesDecoder::<$id, $addr, _>::new(data)?;
                for decoded in decoder.iter() {
                  let decoded = decoded?;
                  msgs.push(Message::<$id, $addr>::from_ref(decoded)?);
                }
              }

              if msgs != messages {
                return Err("messages do not match".into());
              }

              Ok(())
            });

            match res {
              Ok(_) => true,
              Err(e) => {
                println!("error: {}", e);
                false
              }
            }
          }

          #[quickcheck_macros::quickcheck]
          fn [< proto_ $name:snake _ $id:snake _ $addr:snake _fuzzy_without_label >](message: Message<$id, $addr>) -> bool {
            let res: Result<(), Box<dyn std::error::Error>> = futures::executor::block_on(async move {
              let mut encoder = $encoder;
              let messages = [message];
              encoder.with_messages(&messages);
              let data = encoder.encode().collect::<Result<Vec<_>, ProtoEncoderError>>()?;

              let mut msgs = Vec::new();
              let decoder = ProtoDecoder::default();
              for payload in data {
                let data = decoder.decode(BytesMut::from(Bytes::from(payload))).await?;

                let decoder = MessagesDecoder::<$id, $addr, _>::new(data)?;
                for decoded in decoder.iter() {
                  let decoded = decoded?;
                  msgs.push(Message::<$id, $addr>::from_ref(decoded)?);
                }
              }

              if msgs != messages {
                return Err("messages do not match".into());
              }

              Ok(())
            });

            match res {
              Ok(_) => true,
              Err(e) => {
                println!("error: {}", e);
                false
              }
            }
          }

          #[quickcheck_macros::quickcheck]
          fn [< proto_ $name:snake _ $id:snake _ $addr:snake _fuzzy_ignore_label_on_decode >](message: Message<$id, $addr>) -> bool {
            let res: Result<(), Box<dyn std::error::Error>> = futures::executor::block_on(async move {
              let mut encoder = $encoder;
              let label = $label;
              encoder.with_label(&label);

              let messages = [message];
              encoder.with_messages(&messages);
              let data = encoder.encode().collect::<Result<Vec<_>, ProtoEncoderError>>()?;

              let mut msgs = Vec::new();
              let decoder = ProtoDecoder::default();
              for payload in data {
                let data = decoder.decode(BytesMut::from(Bytes::from(payload))).await?;

                let decoder = MessagesDecoder::<$id, $addr, _>::new(data)?;
                for decoded in decoder.iter() {
                  let decoded = decoded?;
                  msgs.push(Message::<$id, $addr>::from_ref(decoded)?);
                }
              }

              if msgs != messages {
                return Err("messages do not match".into());
              }

              Ok(())
            });

            match res {
              Ok(_) => true,
              Err(e) => {
                println!("error: {}", e);
                false
              }
            }
          }
        )*
      )+
    }
  };
  (@single<I, A> $($name:ident($encoder:expr, $pk:expr, $algo:expr)[ $(($id:ident, $addr: ident)), +$(,)? ]),+$(,)?) => {
    paste::paste! {
      $(
        $(
          #[quickcheck_macros::quickcheck]
          fn [< proto_ $name:snake _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
            let res: Result<(), Box<dyn std::error::Error>> = futures::executor::block_on(async move {
              let mut encoder = $encoder;
              let pk = $pk;
              encoder.with_encryption($algo, pk);
              let messages = [message];
              encoder.with_messages(&messages);
              let data = encoder.encode().collect::<Result<Vec<_>, ProtoEncoderError>>()?;

              let mut msgs = Vec::new();
              for payload in data {
                let mut decoder = ProtoDecoder::default();
                decoder.with_encryption(Some(triomphe::Arc::from_iter([pk])));
                let data = decoder.decode(BytesMut::from(Bytes::from(payload))).await?;

                let decoder = MessagesDecoder::<$id, $addr, _>::new(data)?;
                for decoded in decoder.iter() {
                  let decoded = decoded?;
                  msgs.push(Message::<$id, $addr>::from_ref(decoded)?);
                }
              }

              if msgs != messages {
                return Err("messages do not match".into());
              }

              Ok(())
            });

            match res {
              Ok(_) => true,
              Err(e) => {
                println!("error: {}", e);
                false
              }
            }
          }
        )*
      )+
    }
  };
  (@single<I, A> $($name:ident($encoder:expr, $label:expr, $pk:expr, $algo:expr)[ $(($id:ident, $addr: ident)), +$(,)? ]),+$(,)?) => {
    paste::paste! {
      $(
        $(
          #[quickcheck_macros::quickcheck]
          fn [< proto_ $name:snake _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
            let res: Result<(), Box<dyn std::error::Error>> = futures::executor::block_on(async move {
              let mut encoder = $encoder;
              let pk = $pk;
              encoder.with_encryption($algo, pk);
              let label = $label;
              encoder.with_label(&label);
              let messages = [message];
              encoder.with_messages(&messages);
              let data = encoder.encode().collect::<Result<Vec<_>, ProtoEncoderError>>()?;

              let mut msgs = Vec::new();
              let mut decoder = ProtoDecoder::default();
              decoder.with_encryption(Some(triomphe::Arc::from_iter([pk]))).with_label(label);
              for payload in data {
                let data = decoder.decode(BytesMut::from(Bytes::from(payload))).await?;

                let decoder = MessagesDecoder::<$id, $addr, _>::new(data)?;
                for decoded in decoder.iter() {
                  let decoded = decoded?;
                  msgs.push(Message::<$id, $addr>::from_ref(decoded)?);
                }
              }

              if msgs != messages {
                return Err("messages do not match".into());
              }

              Ok(())
            });

            match res {
              Ok(_) => true,
              Err(e) => {
                println!("error: {}", e);
                false
              }
            }
          }
        )*
      )+
    }
  };
}

proto_encoder_decoder_unit_test!(
  @single<I, A> single_message_with_label(ProtoEncoder::new(1500), Label::try_from("test").unwrap()) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ]
);

#[cfg(feature = "crc32")]
proto_encoder_decoder_unit_test!(
  @single<I, A> single_message_with_crc32({
    let mut encoder = ProtoEncoder::new(1500);
    encoder.with_checksum(ChecksumAlgorithm::Crc32);
    encoder
  }, Label::try_from("test").unwrap()) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ]
);

#[cfg(feature = "xxhash32")]
proto_encoder_decoder_unit_test!(
  @single<I, A> single_message_with_xxhash32({
    let mut encoder = ProtoEncoder::new(1500);
    encoder.with_checksum(ChecksumAlgorithm::XxHash32);
    encoder
  }, Label::try_from("test").unwrap()) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ]
);

#[cfg(feature = "xxhash64")]
proto_encoder_decoder_unit_test!(
  @single<I, A> single_message_with_xxhash64({
    let mut encoder = ProtoEncoder::new(1500);
    encoder.with_checksum(ChecksumAlgorithm::XxHash64);
    encoder
  }, Label::try_from("test").unwrap()) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ]
);

#[cfg(feature = "xxhash3")]
proto_encoder_decoder_unit_test!(
  @single<I, A> single_message_with_xxhash3({
    let mut encoder = ProtoEncoder::new(1500);
    encoder.with_checksum(ChecksumAlgorithm::XxHash3);
    encoder
  }, Label::try_from("test").unwrap()) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ]
);

#[cfg(feature = "xxhash3")]
proto_encoder_decoder_unit_test!(
  @single<I, A> single_message_with_murmur3({
    let mut encoder = ProtoEncoder::new(1500);
    encoder.with_checksum(ChecksumAlgorithm::Murmur3);
    encoder
  }, Label::try_from("test").unwrap()) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ]
);

#[cfg(feature = "lz4")]
proto_encoder_decoder_unit_test!(
  @single<I, A> single_message_with_lz4({
    let mut encoder = ProtoEncoder::new(1500);
    encoder.with_compression(CompressAlgorithm::Lz4);
    encoder
  }, Label::try_from("test").unwrap()) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ]
);

#[cfg(feature = "zstd")]
proto_encoder_decoder_unit_test!(
  @single<I, A> single_message_with_zstd({
    let mut encoder = ProtoEncoder::new(1500);
    encoder.with_compression(CompressAlgorithm::Zstd(Default::default()));
    encoder
  }, Label::try_from("test").unwrap()) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ]
);

#[cfg(feature = "snappy")]
proto_encoder_decoder_unit_test!(
  @single<I, A> single_message_with_snappy({
    let mut encoder = ProtoEncoder::new(1500);
    encoder.with_compression(CompressAlgorithm::Snappy);
    encoder
  }, Label::try_from("test").unwrap()) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ]
);

#[cfg(feature = "brotli")]
proto_encoder_decoder_unit_test!(
  @single<I, A> single_message_with_brotli({
    let mut encoder = ProtoEncoder::new(1500);
    encoder.with_compression(CompressAlgorithm::Brotli(Default::default()));
    encoder
  }, Label::try_from("test").unwrap()) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ]
);

#[cfg(feature = "encryption")]
proto_encoder_decoder_unit_test!(
  @single<I, A>
  single_message_encrypted_by_nopadding_aes128(ProtoEncoder::new(1500), SecretKey::random_aes128(), EncryptionAlgorithm::NoPadding) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ],
  single_message_encrypted_by_nopadding_aes192(ProtoEncoder::new(1500), SecretKey::random_aes192(), EncryptionAlgorithm::NoPadding) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ],
  single_message_encrypted_by_nopadding_aes256(ProtoEncoder::new(1500), SecretKey::random_aes256(), EncryptionAlgorithm::NoPadding) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ],
  single_message_encrypted_by_pkcs7_aes128(ProtoEncoder::new(1500), SecretKey::random_aes128(), EncryptionAlgorithm::Pkcs7) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ],
  single_message_encrypted_by_pkcs7_aes192(ProtoEncoder::new(1500), SecretKey::random_aes192(), EncryptionAlgorithm::Pkcs7) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ],
  single_message_encrypted_by_pkcs7_aes256(ProtoEncoder::new(1500), SecretKey::random_aes256(), EncryptionAlgorithm::Pkcs7) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ],
);

#[cfg(feature = "encryption")]
proto_encoder_decoder_unit_test!(
  @single<I, A>
  single_message_encrypted_by_nopadding_aes128_with_label(ProtoEncoder::new(1500), Label::try_from("test").unwrap(), SecretKey::random_aes128(), EncryptionAlgorithm::NoPadding) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ],
  single_message_encrypted_by_nopadding_aes192_with_label(ProtoEncoder::new(1500), Label::try_from("test").unwrap(), SecretKey::random_aes192(), EncryptionAlgorithm::NoPadding) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ],
  single_message_encrypted_by_nopadding_aes256_with_label(ProtoEncoder::new(1500), Label::try_from("test").unwrap(), SecretKey::random_aes256(), EncryptionAlgorithm::NoPadding) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ],
  single_message_encrypted_by_pkcs7_aes128_with_label(ProtoEncoder::new(1500), Label::try_from("test").unwrap(), SecretKey::random_aes128(), EncryptionAlgorithm::Pkcs7) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ],
  single_message_encrypted_by_pkcs7_aes192_with_label(ProtoEncoder::new(1500), Label::try_from("test").unwrap(), SecretKey::random_aes192(), EncryptionAlgorithm::Pkcs7) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ],
  single_message_encrypted_by_pkcs7_aes256_with_label(ProtoEncoder::new(1500), Label::try_from("test").unwrap(), SecretKey::random_aes256(), EncryptionAlgorithm::Pkcs7) [
    (u32, SocketAddrV4), (u32, String), (IpAddr, SocketAddrV4), (IpAddr, String), (String, String), (String, SocketAddrV4)
  ],
);
