use super::{
  Data, EncryptionAlgorithm, Label, Message, MessagesDecoder, ProtoDecoder, ProtoEncoder,
  ProtoEncoderError, RandomSecretKeys,
};

use bytes::{Bytes, BytesMut};
use memberlist_types::SecretKey;
use std::net::SocketAddrV4;
use triomphe::Arc;

fn encode_decode_messages<I, A>(
  parallel: bool,
  algo: EncryptionAlgorithm,
  pk: SecretKey,
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
      .with_encryption(algo, pk)
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
    decoder.with_encryption(Arc::from_iter([pk]));
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
  algo: EncryptionAlgorithm,
  pk: SecretKey,
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
      .with_encryption(algo, pk)
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
    decoder.with_encryption(Arc::from_iter([pk]));
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

fn encode_decode_messages_random_pk<I, A>(
  parallel: bool,
  algo: EncryptionAlgorithm,
  keys: RandomSecretKeys,
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
      .with_encryption(algo, keys.pk)
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
    decoder.with_encryption(keys.keys.clone());
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

fn encode_decode_message_random_pk<I, A>(
  parallel: bool,
  algo: EncryptionAlgorithm,
  keys: RandomSecretKeys,
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
      .with_encryption(algo, keys.pk)
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
    decoder.with_encryption(keys.keys.clone());
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

macro_rules! encryption_unit_test {
  ($name:ident($algo:expr, $pk:expr) [ $(($id:ident, $addr: ident)), +$(,)? ]) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(false, $algo, $pk, messages, Label::try_from("test").unwrap(), true)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(false, $algo, $pk, messages, Label::EMPTY.clone(), false)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
          encode_decode_message(false, $algo, $pk, message, Label::try_from("test").unwrap(), true)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
          encode_decode_message(false, $algo, $pk, message, Label::EMPTY.clone(), false)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(true, $algo, $pk, messages, Label::try_from("test").unwrap(), true)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(true, $algo, $pk, messages, Label::EMPTY.clone(), false)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
          encode_decode_message(true, $algo, $pk, message, Label::try_from("test").unwrap(), true)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
          encode_decode_message(true, $algo, $pk, message, Label::EMPTY.clone(), false)
        }
      )*
    }
  };
}

encryption_unit_test!(nopadding_aes128(EncryptionAlgorithm::NoPadding, SecretKey::random_aes128()) [
  (u32, u32), (u32, String), (String, SocketAddrV4), (String, u32)
]);

encryption_unit_test!(nopadding_aes192(EncryptionAlgorithm::NoPadding, SecretKey::random_aes192()) [
  (u32, u32), (u32, String), (String, SocketAddrV4), (String, u32)
]);

encryption_unit_test!(nopadding_aes256(EncryptionAlgorithm::NoPadding, SecretKey::random_aes256()) [
  (u32, u32), (u32, String), (String, SocketAddrV4), (String, u32)
]);

encryption_unit_test!(pkcs7_aes128(EncryptionAlgorithm::Pkcs7, SecretKey::random_aes128()) [
  (u32, u32), (u32, String), (String, SocketAddrV4), (String, u32)
]);

encryption_unit_test!(pkcs7_aes192(EncryptionAlgorithm::Pkcs7, SecretKey::random_aes192()) [
  (u32, u32), (u32, String), (String, SocketAddrV4), (String, u32)
]);

encryption_unit_test!(pkcs7_aes256(EncryptionAlgorithm::Pkcs7, SecretKey::random_aes256()) [
  (u32, u32), (u32, String), (String, SocketAddrV4), (String, u32)
]);

macro_rules! encryption_random_pk_unit_test {
  ($name:ident($algo:expr) [ $(($id:ident, $addr: ident)), +$(,)? ]) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_random_pk_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          keys: RandomSecretKeys,
        ) -> bool {
          encode_decode_messages_random_pk(false, $algo, keys, messages, Label::try_from("test").unwrap(), true)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_random_pk_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          keys: RandomSecretKeys,
        ) -> bool {
          encode_decode_messages_random_pk(false, $algo, keys, messages, Label::EMPTY.clone(), false)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_random_pk_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          message: Message<$id, $addr>,
          keys: RandomSecretKeys,
        ) -> bool {
          encode_decode_message_random_pk(false, $algo, keys, message, Label::try_from("test").unwrap(), true)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_random_pk_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          message: Message<$id, $addr>,
          keys: RandomSecretKeys,
        ) -> bool {
          encode_decode_message_random_pk(false, $algo, keys, message, Label::EMPTY.clone(), false)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_random_pk_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          keys: RandomSecretKeys,
        ) -> bool {
          encode_decode_messages_random_pk(true, $algo, keys, messages, Label::try_from("test").unwrap(), true)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_random_pk_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          keys: RandomSecretKeys,
        ) -> bool {
          encode_decode_messages_random_pk(true, $algo, keys, messages, Label::EMPTY.clone(), false)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_random_pk_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          message: Message<$id, $addr>,
          keys: RandomSecretKeys,
        ) -> bool {
          encode_decode_message_random_pk(true, $algo, keys, message, Label::try_from("test").unwrap(), true)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_random_pk_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          message: Message<$id, $addr>,
          keys: RandomSecretKeys,
        ) -> bool {
          encode_decode_message_random_pk(true, $algo, keys, message, Label::EMPTY.clone(), false)
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
