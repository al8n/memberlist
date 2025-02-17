use super::{ChecksumAlgorithm, Data, Label, Message, ProtoDecoder, ProtoEncoder};

use std::net::{IpAddr, SocketAddrV4};

fn encode_decode_messages<I, A>(
  parallel: bool,
  algo: ChecksumAlgorithm,
  messages: Vec<Message<I, A>>,
  label: Label,
  check_label: bool,
  stream: bool,
) -> bool
where
  I: Data + PartialEq + Clone + 'static,
  A: Data + PartialEq + Clone + 'static,
{
  let encoder = ProtoEncoder::new(1500)
    .with_messages(messages)
    .with_checksum(algo)
    .with_label(label.clone());

  let mut decoder = ProtoDecoder::new();
  if check_label {
    decoder.with_label(label);
  }
  super::encode_decode_roundtrip::<_, _, _, agnostic_lite::tokio::TokioRuntime>(
    parallel, stream, encoder, decoder,
  )
}

fn encode_decode_message<I, A>(
  parallel: bool,
  algo: ChecksumAlgorithm,
  message: Message<I, A>,
  label: Label,
  check_label: bool,
  stream: bool,
) -> bool
where
  I: Data + PartialEq + Clone + 'static,
  A: Data + PartialEq + Clone + 'static,
{
  let encoder = ProtoEncoder::new(1500)
    .with_messages([message])
    .with_checksum(algo)
    .with_label(label.clone());

  let mut decoder = ProtoDecoder::new();
  if check_label {
    decoder.with_label(label);
  }
  super::encode_decode_roundtrip::<_, _, _, agnostic_lite::tokio::TokioRuntime>(
    parallel, stream, encoder, decoder,
  )
}

macro_rules! checksum_unit_test {
  ($name:ident($algo:expr) [ $(($id:ident, $addr: ident)), +$(,)? ]) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          stream: bool,
          parallel: bool,
        ) -> bool {
          encode_decode_messages(parallel, $algo, messages, Label::try_from("test").unwrap(), true, stream)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          stream: bool,
          parallel: bool,
        ) -> bool {
          encode_decode_messages(parallel, $algo, messages, Label::EMPTY.clone(), false, stream)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          message: Message<$id, $addr>,
          stream: bool,
          parallel: bool,
        ) -> bool {
          encode_decode_message(parallel, $algo, message, Label::try_from("test").unwrap(), true, stream)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          message: Message<$id, $addr>,
          stream: bool,
          parallel: bool,
        ) -> bool {
          encode_decode_message(parallel, $algo, message, Label::EMPTY.clone(), false, stream)
        }
      )*
    }
  };
}

#[cfg(feature = "crc32")]
checksum_unit_test!(crc32(ChecksumAlgorithm::Crc32)[(IpAddr, SocketAddrV4)]);

#[cfg(feature = "xxhash32")]
checksum_unit_test!(xxhash32(ChecksumAlgorithm::XxHash32)[(IpAddr, SocketAddrV4)]);

#[cfg(feature = "xxhash64")]
checksum_unit_test!(xxhash64(ChecksumAlgorithm::XxHash64)[(IpAddr, SocketAddrV4)]);

#[cfg(feature = "xxhash3")]
checksum_unit_test!(xxhash3(ChecksumAlgorithm::XxHash3)[(IpAddr, SocketAddrV4)]);

#[cfg(feature = "murmur3")]
checksum_unit_test!(murmur3(ChecksumAlgorithm::Murmur3)[(IpAddr, SocketAddrV4)]);
