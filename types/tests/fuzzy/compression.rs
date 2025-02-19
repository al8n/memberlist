use super::{CompressAlgorithm, Data, Label, Message, ProtoDecoder, ProtoEncoder};

use std::net::SocketAddrV4;

fn encode_decode_messages<I, A>(
  parallel: bool,
  algo: CompressAlgorithm,
  messages: Vec<Message<I, A>>,
  label: Label,
  check_label: bool,
  stream: bool,
  overhead: u8,
) -> bool
where
  I: Data + PartialEq + Clone + 'static,
  A: Data + PartialEq + Clone + 'static,
{
  let encoder = ProtoEncoder::new(1500)
    .with_messages(messages)
    .with_compression(algo)
    .with_label(label.clone());

  let mut decoder = ProtoDecoder::new();
  if check_label {
    decoder.with_label(label);
  }
  super::encode_decode_roundtrip::<_, _, _, agnostic_lite::tokio::TokioRuntime>(
    overhead, parallel, stream, encoder, decoder,
  )
}

fn encode_decode_message<I, A>(
  parallel: bool,
  algo: CompressAlgorithm,
  message: Message<I, A>,
  label: Label,
  check_label: bool,
  stream: bool,
  overhead: u8,
) -> bool
where
  I: Data + PartialEq + Clone + 'static,
  A: Data + PartialEq + Clone + 'static,
{
  let encoder = ProtoEncoder::new(1500)
    .with_messages([message])
    .with_compression(algo)
    .with_label(label.clone());

  let mut decoder = ProtoDecoder::new();
  if check_label {
    decoder.with_label(label);
  }
  super::encode_decode_roundtrip::<_, _, _, agnostic_lite::tokio::TokioRuntime>(
    overhead, parallel, stream, encoder, decoder,
  )
}

macro_rules! compression_unit_test {
  ($name:ident($algo:expr) [ $(($id:ident, $addr: ident)), +$(,)? ]) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_messages(parallel, $algo, messages, Label::try_from("test").unwrap(), true, stream, overhead)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_messages(parallel, $algo, messages, Label::EMPTY.clone(), false, stream, overhead)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          message: Message<$id, $addr>,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_message(parallel, $algo, message, Label::try_from("test").unwrap(), true, stream, overhead)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          message: Message<$id, $addr>,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_message(parallel, $algo, message, Label::EMPTY.clone(), false, stream, overhead)
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

#[cfg(feature = "brotli")]
compression_unit_test!(brotli(CompressAlgorithm::Brotli(Default::default())) [
  (u32, u32), (u32, String), (String, SocketAddrV4), (String, u32)
]);
