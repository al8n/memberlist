#![allow(clippy::too_many_arguments)]

use super::{
  Data, EncryptionAlgorithm, Label, Message, ProtoDecoder, ProtoEncoder, RandomSecretKeys,
};

fn encode_decode_messages<I, A>(
  parallel: bool,
  algo: EncryptionAlgorithm,
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
    .with_encryption(algo, keys.pk)
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
  algo: EncryptionAlgorithm,
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
    .with_encryption(algo, keys.pk)
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

macro_rules! encryption_random_pk_unit_test {
  ($name:ident($algo:expr) [ $(($id:ident, $addr: ident)), +$(,)? ]) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_random_pk_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          keys: RandomSecretKeys,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_messages(parallel, $algo, keys, messages, Label::try_from("test").unwrap(), true, stream, overhead)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_random_pk_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          keys: RandomSecretKeys,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_messages(parallel, $algo, keys, messages, Label::EMPTY.clone(), false, stream, overhead)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_random_pk_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          message: Message<$id, $addr>,
          keys: RandomSecretKeys,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_message(parallel, $algo, keys, message, Label::try_from("test").unwrap(), true, stream, overhead)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_random_pk_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          message: Message<$id, $addr>,
          keys: RandomSecretKeys,
          stream: bool,
          parallel: bool,
          overhead: u8,
        ) -> bool {
          encode_decode_message(parallel, $algo, keys, message, Label::EMPTY.clone(), false, stream, overhead)
        }
      )*
    }
  };
}

encryption_random_pk_unit_test!(nopadding_aes128(EncryptionAlgorithm::NoPadding) [
  (u32, u32),
]);

encryption_random_pk_unit_test!(nopadding_aes192(EncryptionAlgorithm::NoPadding) [
  (u32, u32),
]);

encryption_random_pk_unit_test!(nopadding_aes256(EncryptionAlgorithm::NoPadding) [
  (u32, u32),
]);

encryption_random_pk_unit_test!(pkcs7_aes128(EncryptionAlgorithm::Pkcs7) [
  (u32, u32),
]);

encryption_random_pk_unit_test!(pkcs7_aes192(EncryptionAlgorithm::Pkcs7) [
  (u32, u32),
]);

encryption_random_pk_unit_test!(pkcs7_aes256(EncryptionAlgorithm::Pkcs7) [
  (u32, u32),
]);
