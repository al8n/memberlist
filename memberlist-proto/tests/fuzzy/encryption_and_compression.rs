#![allow(clippy::too_many_arguments)]

use super::{
  CompressAlgorithm, Data, EncryptionAlgorithm, Label, Message, ProtoDecoder, ProtoEncoder,
  RandomSecretKeys,
};

use std::net::{IpAddr, SocketAddrV4};

fn encode_decode_messages<I, A>(
  parallel: bool,
  compress_algo: CompressAlgorithm,
  encryption_algo: EncryptionAlgorithm,
  keys: RandomSecretKeys,
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
    .with_compression(compress_algo)
    .with_encryption(encryption_algo, keys.pk)
    .with_label(label.clone());

  let mut decoder = ProtoDecoder::new();
  decoder.with_encryption(keys.keys.clone());
  if check_label {
    decoder.with_label(label);
  }
  super::encode_decode_roundtrip::<_, _, _, agnostic_lite::tokio::TokioRuntime>(
    overhead, parallel, stream, encoder, decoder,
  )
}

fn encode_decode_message<I, A>(
  parallel: bool,
  compress_algo: CompressAlgorithm,
  encryption_algo: EncryptionAlgorithm,
  keys: RandomSecretKeys,
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
    .with_compression(compress_algo)
    .with_encryption(encryption_algo, keys.pk)
    .with_label(label.clone());

  let mut decoder = ProtoDecoder::new();
  decoder.with_encryption(keys.keys.clone());
  if check_label {
    decoder.with_label(label);
  }
  super::encode_decode_roundtrip::<_, _, _, agnostic_lite::tokio::TokioRuntime>(
    overhead, parallel, stream, encoder, decoder,
  )
}

macro_rules! encryption_and_compression_unit_test {
  ($name:ident [ $(($id:ident, $addr: ident)), +$(,)? ]) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          compress_algo: CompressAlgorithm,
          encryption_algo: EncryptionAlgorithm,
          keys: RandomSecretKeys,
          messages: Vec<Message<$id, $addr>>,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_messages(parallel, compress_algo, encryption_algo, keys,  messages, Label::try_from("test").unwrap(), true, stream, overhead)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          compress_algo: CompressAlgorithm,
          encryption_algo: EncryptionAlgorithm,
          keys: RandomSecretKeys,
          messages: Vec<Message<$id, $addr>>,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_messages(parallel, compress_algo, encryption_algo, keys,  messages, Label::EMPTY.clone(), false, stream, overhead,)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          compress_algo: CompressAlgorithm,
          encryption_algo: EncryptionAlgorithm,
          keys: RandomSecretKeys,
          message: Message<$id, $addr>,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_message(parallel, compress_algo, encryption_algo, keys,  message, Label::try_from("test").unwrap(), true, stream, overhead,)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          compress_algo: CompressAlgorithm,
          encryption_algo: EncryptionAlgorithm,
          keys: RandomSecretKeys,
          message: Message<$id, $addr>,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_message(parallel, compress_algo, encryption_algo, keys,  message, Label::EMPTY.clone(), false, stream, overhead,)
        }
      )*
    }
  };
}

encryption_and_compression_unit_test!(encryption_and_compression[(IpAddr, SocketAddrV4)]);
