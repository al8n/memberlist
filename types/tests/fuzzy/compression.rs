use super::{
  CompressAlgorithm, Data, Label, Message, MessagesDecoder, ProtoDecoder, ProtoEncoder,
  ProtoEncoderError,
};

use bytes::{Bytes, BytesMut};
use std::net::SocketAddrV4;

fn encode_decode_messages<I, A>(
  parallel: bool,
  algo: CompressAlgorithm,
  messages: Vec<Message<I, A>>,
  label: Label,
  check_label: bool,
) -> bool
where
  I: Data + PartialEq,
  A: Data + PartialEq,
{
  let res = super::run(async move {
    let encoder = ProtoEncoder::new(1500)
      .with_messages(&messages)
      .with_compression(algo)
      .with_label(label.clone());

    let data = {
      cfg_if::cfg_if! {
        if #[cfg(feature = "rayon")] {
          use rayon::iter::ParallelIterator;

          if parallel {
            encoder
              .rayon_encode()
              .map(|res| res)
              .collect::<Result<Vec<_>, ProtoEncoderError>>()?
          } else {
            encoder
              .encode()
              .collect::<Result<Vec<_>, ProtoEncoderError>>()?
          }
        } else {
          encoder
            .encode()
            .collect::<Result<Vec<_>, ProtoEncoderError>>()?
        }
      }
    };

    let mut msgs = Vec::new();
    let mut decoder = ProtoDecoder::default();
    if check_label {
      decoder.with_label(label);
    }

    for payload in data {
      let data = decoder
        .decode::<agnostic_lite::tokio::TokioRuntime>(BytesMut::from(Bytes::from(payload)))
        .await?;
      let decoder = MessagesDecoder::<I, A, _>::new(data)?;
      for decoded in decoder.iter() {
        let decoded = decoded?;
        msgs.push(Message::<I, A>::from_ref(decoded)?);
      }
    }

    assert_eq!(msgs, messages);

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

fn encode_decode_message<I, A>(
  parallel: bool,
  algo: CompressAlgorithm,
  message: Message<I, A>,
  label: Label,
  check_label: bool,
) -> bool
where
  I: Data + PartialEq,
  A: Data + PartialEq,
{
  let res = super::run(async move {
    let messages = [message];
    let encoder = ProtoEncoder::new(1500)
      .with_messages(&messages)
      .with_compression(algo)
      .with_label(label.clone());

    let data = {
      cfg_if::cfg_if! {
        if #[cfg(feature = "rayon")] {
          use rayon::iter::ParallelIterator;

          if parallel {
            encoder
              .rayon_encode()
              .map(|res| res)
              .collect::<Result<Vec<_>, ProtoEncoderError>>()?
          } else {
            encoder
              .encode()
              .collect::<Result<Vec<_>, ProtoEncoderError>>()?
          }
        } else {
          encoder
            .encode()
            .collect::<Result<Vec<_>, ProtoEncoderError>>()?
        }
      }
    };

    let mut msgs = Vec::new();
    let mut decoder = ProtoDecoder::default();
    if check_label {
      decoder.with_label(label);
    }

    for payload in data {
      let data = decoder
        .decode::<agnostic_lite::tokio::TokioRuntime>(BytesMut::from(Bytes::from(payload)))
        .await?;
      let decoder = MessagesDecoder::<I, A, _>::new(data)?;
      for decoded in decoder.iter() {
        let decoded = decoded?;
        msgs.push(Message::<I, A>::from_ref(decoded)?);
      }
    }

    assert_eq!(msgs, messages);

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

macro_rules! compression_unit_test {
  ($name:ident($algo:expr) [ $(($id:ident, $addr: ident)), +$(,)? ]) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(false, $algo, messages, Label::try_from("test").unwrap(), true)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(false, $algo, messages, Label::EMPTY.clone(), false)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
          encode_decode_message(false, $algo, message, Label::try_from("test").unwrap(), true)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
          encode_decode_message(false, $algo, message, Label::EMPTY.clone(), false)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(true, $algo, messages, Label::try_from("test").unwrap(), true)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(true, $algo, messages, Label::EMPTY.clone(), false)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
          encode_decode_message(true, $algo, message, Label::try_from("test").unwrap(), true)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
          encode_decode_message(true, $algo, message, Label::EMPTY.clone(), false)
        }
      )*
    }
  };
}

#[cfg(feature = "lz4")]
compression_unit_test!(lz4(CompressAlgorithm::Lz4) [
  (u32, u32), (u32, String), (String, SocketAddrV4), (String, u32)
]);

#[cfg(feature = "zstd")]
compression_unit_test!(zstd(CompressAlgorithm::Zstd(Default::default())) [
  (u32, u32), (u32, String), (String, SocketAddrV4), (String, u32)
]);

#[cfg(feature = "snappy")]
compression_unit_test!(snappy(CompressAlgorithm::Snappy) [
  (u32, u32), (u32, String), (String, SocketAddrV4), (String, u32)
]);

// #[cfg(feature = "brotli")]
// compression_unit_test!(brotli(CompressAlgorithm::Brotli(Default::default())) [
//   (u32, u32), (u32, String), (String, SocketAddrV4), (String, u32)
// ]);
