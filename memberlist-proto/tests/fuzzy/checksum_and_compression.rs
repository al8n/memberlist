#![allow(clippy::too_many_arguments)]

use super::{
  ChecksumAlgorithm, CompressAlgorithm, Data, Label, Message, ProtoDecoder, ProtoEncoder,
};

use std::net::{IpAddr, SocketAddrV4};

fn encode_decode_messages<I, A>(
  parallel: bool,
  checksum_algo: ChecksumAlgorithm,
  compress_algo: CompressAlgorithm,
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
    .with_checksum(checksum_algo)
    .with_compression(compress_algo)
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
  checksum_algo: ChecksumAlgorithm,
  compress_algo: CompressAlgorithm,
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
    .with_checksum(checksum_algo)
    .with_compression(compress_algo)
    .with_label(label.clone());

  let mut decoder = ProtoDecoder::new();
  if check_label {
    decoder.with_label(label);
  }
  super::encode_decode_roundtrip::<_, _, _, agnostic_lite::tokio::TokioRuntime>(
    overhead, parallel, stream, encoder, decoder,
  )
}

macro_rules! checksum_and_compression_unit_test {
  ($name:ident [ $(($id:ident, $addr: ident)), +$(,)? ]) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,
          messages: Vec<Message<$id, $addr>>,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_messages(parallel, checksum_algo, compress_algo,  messages, Label::try_from("test").unwrap(), true, stream, overhead)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,
          messages: Vec<Message<$id, $addr>>,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_messages(parallel, checksum_algo, compress_algo, messages, Label::EMPTY.clone(), false, stream, overhead)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,
          message: Message<$id, $addr>,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_message(parallel, checksum_algo, compress_algo, message, Label::try_from("test").unwrap(), true, stream, overhead)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,
          message: Message<$id, $addr>,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_message(parallel, checksum_algo, compress_algo, message, Label::EMPTY.clone(), false, stream, overhead)
        }
      )*
    }
  };
}

checksum_and_compression_unit_test!(checksum_and_compression[(IpAddr, SocketAddrV4)]);
