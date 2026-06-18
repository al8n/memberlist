//! UDP gossip-plane transform-pipeline benchmark.
//!
//! The gossip (UDP) plane runs a serial transform inline in the driver pump:
//! egress `encode → compress → encrypt`, ingress `decrypt → decompress → parse`.
//! `handle_packet` (the SWIM state transition) is cheap by design. This
//! benchmark measures the per-packet cost of each stage and of the full
//! pipeline, on a small probe (a `Ping`) and a near-MTU gossip batch (a compound
//! of `Alive`s) — the data that establishes the transform is microsecond-scale,
//! so running it inline on the pump is sound.
//!
//! Run:
//!   cargo bench -p memberlist-proto --bench transform_pipeline \
//!     --features std,lz4,aes-gcm,chacha20-poly1305

#[cfg(all(
  feature = "std",
  feature = "lz4",
  feature = "aes-gcm",
  feature = "chacha20-poly1305"
))]
mod bench {
  use core::net::SocketAddr;

  use bytes::Bytes;
  use std::hint::black_box;

  use criterion::{Criterion, Throughput};
  use smol_str::SmolStr;

  use memberlist_proto::{
    codec::{EncodeOptions, encode_outgoing, encode_outgoing_compound, parse_messages},
    compression::{
      CompressAlgorithm, CompressionOptions, CompressionOutput, encode_compressed_frame,
    },
    encryption::{
      EncryptAlgorithm, EncryptionOptions, Keyring, SecretKey, encode_encrypted_frame, encrypt,
    },
    framing::unwrap_transforms_with_encryption,
    typed::{Alive, DelegateVersion, Message, Node, Ping, ProtocolVersion},
  };

  type I = SmolStr;
  type A = SocketAddr;

  /// The unwrap decompression ceiling — generous; the bench payloads are well
  /// under a realistic gossip MTU.
  const CEIL: usize = 64 * 1024;

  fn node(i: usize) -> Node<I, A> {
    Node::new(
      SmolStr::new(format!("node-{i:04}")),
      format!(
        "10.{}.{}.{}:7946",
        (i >> 16) & 0xff,
        (i >> 8) & 0xff,
        i & 0xff
      )
      .parse()
      .unwrap(),
    )
  }

  fn alive(i: usize) -> Message<I, A> {
    Message::Alive(
      Alive::new(i as u32 + 1, node(i))
        .with_protocol_version(ProtocolVersion::V1)
        .with_delegate_version(DelegateVersion::V1),
    )
  }

  fn ping() -> Message<I, A> {
    Message::Ping(Ping::new(42, node(1), node(2)))
  }

  /// Replicate the driver's `compress_gossip` (streams/mod.rs): attempt
  /// compression, keep the framed form only if it shrinks the datagram, else
  /// emit plain. The lz4 compress cost is paid either way (as in the driver).
  fn compress_like_driver(opts: &CompressionOptions, datagram: &[u8]) -> Vec<u8> {
    match opts.apply(datagram) {
      Ok(CompressionOutput::Compressed(packed)) => {
        let wrapped = encode_compressed_frame(
          opts
            .algorithm()
            .expect("a Compressed outcome implies an algorithm"),
          datagram.len(),
          &packed,
        );
        if wrapped.len() < datagram.len() {
          wrapped
        } else {
          datagram.to_vec()
        }
      }
      _ => datagram.to_vec(),
    }
  }

  /// Replicate the driver's `encrypt_gossip`.
  fn encrypt_like_driver(opts: &EncryptionOptions, datagram: &[u8]) -> Vec<u8> {
    let key = opts.keyring().expect("keyring set").primary_ref();
    encode_encrypted_frame(key.algorithm(), key, datagram).expect("encrypt")
  }

  /// Replicate the driver's `decrypt_gossip` (decrypt + decompress in one pass).
  fn unwrap_like_driver(opts: &EncryptionOptions, datagram: &[u8]) -> Vec<u8> {
    unwrap_transforms_with_encryption(datagram, CEIL, opts)
      .expect("unwrap")
      .into_owned()
  }

  pub fn run() {
    let mut c = Criterion::default().configure_from_args();

    let lz4 = CompressionOptions::new().with_algorithm(CompressAlgorithm::Lz4);
    let aes = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x37; 32])));
    let chacha =
      EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::ChaCha20Poly1305([0x37; 32])));
    let opts = EncodeOptions::new(None);

    let ping_msg = ping();
    let batch: Vec<Message<I, A>> = (0..12).map(alive).collect();

    let ping_plain = encode_outgoing(&ping_msg, &opts).expect("encode ping");
    let batch_plain = encode_outgoing_compound(&batch, &opts).expect("encode batch");
    eprintln!(
      "payload sizes: ping plain = {} B, batch plain = {} B ({} Alives)",
      ping_plain.len(),
      batch_plain.len(),
      batch.len(),
    );

    // ── Full egress: encode → compress → encrypt ───────────────────────────
    {
      let mut g = c.benchmark_group("egress_full");
      g.throughput(Throughput::Elements(1));
      g.bench_function("ping/lz4+aes256", |b| {
        b.iter(|| {
          let plain = encode_outgoing(black_box(&ping_msg), &opts).unwrap();
          black_box(encrypt_like_driver(
            &aes,
            &compress_like_driver(&lz4, &plain),
          ))
        })
      });
      g.bench_function("batch/lz4+aes256", |b| {
        b.iter(|| {
          let plain = encode_outgoing_compound(black_box(&batch), &opts).unwrap();
          black_box(encrypt_like_driver(
            &aes,
            &compress_like_driver(&lz4, &plain),
          ))
        })
      });
      g.bench_function("batch/lz4+chacha20", |b| {
        b.iter(|| {
          let plain = encode_outgoing_compound(black_box(&batch), &opts).unwrap();
          black_box(encrypt_like_driver(
            &chacha,
            &compress_like_driver(&lz4, &plain),
          ))
        })
      });
      g.bench_function("batch/no-transform(encode only)", |b| {
        b.iter(|| black_box(encode_outgoing_compound(black_box(&batch), &opts).unwrap()))
      });
      g.finish();
    }

    // ── Full ingress: decrypt+decompress → parse ───────────────────────────
    let ping_wire = encrypt_like_driver(&aes, &compress_like_driver(&lz4, &ping_plain));
    let batch_comp = compress_like_driver(&lz4, &batch_plain);
    let batch_wire_aes = encrypt_like_driver(&aes, &batch_comp);
    let batch_wire_chacha = encrypt_like_driver(&chacha, &batch_comp);
    eprintln!(
      "on-wire sizes: ping = {} B, batch(aes) = {} B (compressed batch = {} B)",
      ping_wire.len(),
      batch_wire_aes.len(),
      batch_comp.len(),
    );
    {
      let mut g = c.benchmark_group("ingress_full");
      g.throughput(Throughput::Elements(1));
      g.bench_function("ping/lz4+aes256", |b| {
        b.iter(|| {
          let plain = unwrap_like_driver(&aes, black_box(&ping_wire));
          black_box(parse_messages::<I, A>(Bytes::from(plain)).unwrap())
        })
      });
      g.bench_function("batch/lz4+aes256", |b| {
        b.iter(|| {
          let plain = unwrap_like_driver(&aes, black_box(&batch_wire_aes));
          black_box(parse_messages::<I, A>(Bytes::from(plain)).unwrap())
        })
      });
      g.bench_function("batch/lz4+chacha20", |b| {
        b.iter(|| {
          let plain = unwrap_like_driver(&chacha, black_box(&batch_wire_chacha));
          black_box(parse_messages::<I, A>(Bytes::from(plain)).unwrap())
        })
      });
      g.bench_function("batch/no-transform(parse only)", |b| {
        b.iter(|| black_box(parse_messages::<I, A>(black_box(batch_plain.clone())).unwrap()))
      });
      g.finish();
    }

    // ── Per-stage breakdown on the near-MTU batch ──────────────────────────
    {
      let mut g = c.benchmark_group("stage_batch");
      g.throughput(Throughput::Elements(1));
      g.bench_function("1_encode", |b| {
        b.iter(|| black_box(encode_outgoing_compound(black_box(&batch), &opts).unwrap()))
      });
      g.bench_function("2_compress_lz4", |b| {
        b.iter(|| black_box(compress_like_driver(&lz4, black_box(&batch_plain))))
      });
      g.bench_function("3_encrypt_aes256", |b| {
        b.iter(|| black_box(encrypt_like_driver(&aes, black_box(&batch_comp))))
      });
      g.bench_function("3_encrypt_chacha20", |b| {
        b.iter(|| black_box(encrypt_like_driver(&chacha, black_box(&batch_comp))))
      });
      // Fixed-nonce variants isolate the raw AEAD math from the per-packet
      // `getrandom` nonce draw the framed `encode_encrypted_frame` does above:
      // the delta (framed − fixed) is the nonce/RNG + alloc overhead.
      let aes_key = SecretKey::Aes256([0x37; 32]);
      let chacha_key = SecretKey::ChaCha20Poly1305([0x37; 32]);
      g.bench_function("3_encrypt_aes256_fixed_nonce", |b| {
        b.iter(|| {
          black_box(
            encrypt(
              EncryptAlgorithm::AesGcm,
              &aes_key,
              &[0u8; 12],
              black_box(&batch_comp),
              &[13, 1],
            )
            .unwrap(),
          )
        })
      });
      g.bench_function("3_encrypt_chacha20_fixed_nonce", |b| {
        b.iter(|| {
          black_box(
            encrypt(
              EncryptAlgorithm::ChaCha20Poly1305,
              &chacha_key,
              &[0u8; 12],
              black_box(&batch_comp),
              &[13, 2],
            )
            .unwrap(),
          )
        })
      });
      g.bench_function("4_decrypt+decompress_aes256", |b| {
        b.iter(|| black_box(unwrap_like_driver(&aes, black_box(&batch_wire_aes))))
      });
      g.bench_function("5_parse", |b| {
        b.iter(|| black_box(parse_messages::<I, A>(black_box(batch_plain.clone())).unwrap()))
      });
      g.finish();
    }

    c.final_summary();
  }
}

fn main() {
  #[cfg(all(
    feature = "std",
    feature = "lz4",
    feature = "aes-gcm",
    feature = "chacha20-poly1305"
  ))]
  bench::run();
  #[cfg(not(all(
    feature = "std",
    feature = "lz4",
    feature = "aes-gcm",
    feature = "chacha20-poly1305"
  )))]
  eprintln!(
    "transform_pipeline requires --features \
     std,lz4,aes-gcm,chacha20-poly1305"
  );
}
