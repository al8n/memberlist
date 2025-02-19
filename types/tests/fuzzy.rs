use core::{
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
  time::Duration,
};
use memberlist_types::{
  Ack, Alive, ChecksumAlgorithm, CompressAlgorithm, Data, Dead, EncryptionAlgorithm, ErrorResponse,
  IndirectPing, Label, Message, MessagesDecoder, Nack, Ping, ProtoDecoder, ProtoEncoder,
  ProtoEncoderError, PushNodeState, PushPull, SecretKey, Suspect,
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

fn run<F>(fut: F) -> F::Output
where
  F: core::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>>,
{
  tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap()
    .block_on(fut)
}

fn encode_decode_roundtrip<I, A, M, R>(
  overhead: u8,
  parallel: bool,
  stream: bool,
  encoder: ProtoEncoder<I, A, M>,
  decoder: ProtoDecoder,
) -> bool
where
  I: Data + PartialEq + 'static,
  A: Data + PartialEq + 'static,
  M: AsRef<[Message<I, A>]> + Clone + Send + Sync + 'static,
  R: agnostic_lite::RuntimeLite,
{
  let res = run(async move {
    let encoder = encoder.with_overhead(overhead as usize);
    let messages = encoder.messages().clone();
    let data = {
      if parallel {
        cfg_if::cfg_if! {
          if #[cfg(feature = "rayon")] {
            use rayon::iter::ParallelIterator;

            encoder
              .rayon_encode()
              .map(|res| res)
              .collect::<Result<Vec<_>, ProtoEncoderError>>()?
          } else {
            encoder.blocking_encode::<R>().await.collect::<Result<Vec<_>, ProtoEncoderError>>()?
          }
        }
      } else {
        encoder
          .encode()
          .collect::<Result<Vec<_>, ProtoEncoderError>>()?
      }
    };

    let mut msgs = Vec::new();

    for payload in data {
      let data = if !stream {
        decoder.decode::<R>(payload.split().1).await?
      } else {
        decoder
          .decode_from_reader::<_, R>(&mut futures::io::Cursor::new(payload.split().1))
          .await?
      };
      let decoder = MessagesDecoder::<I, A, _>::new(data)?;
      for decoded in decoder.iter() {
        let decoded = decoded?;
        msgs.push(Message::<I, A>::from_ref(decoded)?);
      }
    }

    if msgs.as_slice().ne(messages.as_ref()) {
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

#[cfg(feature = "encryption")]
#[derive(Debug, Clone)]
struct RandomSecretKeys {
  keys: triomphe::Arc<[SecretKey]>,
  pk: SecretKey,
}

#[cfg(feature = "encryption")]
impl quickcheck::Arbitrary for RandomSecretKeys {
  fn arbitrary(g: &mut quickcheck::Gen) -> Self {
    let num = (u8::arbitrary(g) % 10) as usize + 1;
    let mut keys = Vec::with_capacity(num);

    for _ in 0..num {
      keys.push(SecretKey::arbitrary(g));
    }

    let pk = *g.choose(&keys).unwrap();

    Self {
      keys: triomphe::Arc::from(keys),
      pk,
    }
  }
}

#[path = "fuzzy/plain.rs"]
mod plain;

#[cfg(any(
  feature = "zstd",
  feature = "lz4",
  feature = "snappy",
  feature = "brotli",
))]
#[path = "fuzzy/compression.rs"]
mod compression;

#[cfg(any(
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3",
))]
#[path = "fuzzy/checksum.rs"]
mod checksum;

#[cfg(feature = "encryption")]
#[path = "fuzzy/encryption.rs"]
mod encryption;

#[cfg(all(
  feature = "encryption",
  feature = "zstd",
  feature = "lz4",
  feature = "snappy",
  feature = "brotli",
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3",
))]
#[path = "fuzzy/all.rs"]
mod all;

#[cfg(all(
  feature = "zstd",
  feature = "lz4",
  feature = "snappy",
  feature = "brotli",
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3",
))]
#[path = "fuzzy/checksum_and_compression.rs"]
mod checksum_and_compression;

#[cfg(all(
  feature = "encryption",
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3",
))]
#[path = "fuzzy/checksum_and_encryption.rs"]
mod checksum_and_encryption;

#[cfg(all(
  feature = "encryption",
  feature = "zstd",
  feature = "lz4",
  feature = "snappy",
  feature = "brotli",
))]
#[path = "fuzzy/encryption_and_compression.rs"]
mod encryption_and_compression;
