#![allow(clippy::too_many_arguments)]

use super::{
  ChecksumAlgorithm, CompressAlgorithm, Data, EncryptionAlgorithm, Label, Message, MessagesDecoder,
  ProtoDecoder, ProtoEncoder, ProtoEncoderError, RandomSecretKeys,
};

use agnostic_lite::tokio::TokioRuntime;
use bytes::{Bytes, BytesMut};
use std::net::{IpAddr, SocketAddrV4};

fn encode_decode_messages<I, A>(
  parallel: bool,
  checksum_algo: ChecksumAlgorithm,
  compress_algo: CompressAlgorithm,
  encryption_algo: EncryptionAlgorithm,
  keys: RandomSecretKeys,
  messages: Vec<Message<I, A>>,
  label: Label,
  check_label: bool,
  stream: bool,
) -> bool
where
  I: Data + PartialEq,
  A: Data + PartialEq,
{
  let res =
    super::run(async move {
      let encoder = ProtoEncoder::new(1500)
        .with_messages(&messages)
        .with_checksum(checksum_algo)
        .with_compression(compress_algo)
        .with_encryption(encryption_algo, keys.pk)
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
        let data = if !stream { 
          decoder
            .decode::<TokioRuntime>(BytesMut::from(Bytes::from(payload)))
            .await?
        } else {
          decoder
            .decode_from_reader::<_, TokioRuntime>(&mut futures::io::Cursor::new(payload))
            .await?
        };
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
  checksum_algo: ChecksumAlgorithm,
  compress_algo: CompressAlgorithm,
  encryption_algo: EncryptionAlgorithm,
  keys: RandomSecretKeys,
  message: Message<I, A>,
  label: Label,
  check_label: bool,
  stream: bool,
) -> bool
where
  I: Data + PartialEq,
  A: Data + PartialEq,
{
  let res = super::run(async move {
    let messages = [message];
    let encoder = ProtoEncoder::new(1500)
      .with_messages(&messages)
      .with_checksum(checksum_algo)
      .with_compression(compress_algo)
      .with_encryption(encryption_algo, keys.pk)
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
      let data = if !stream { 
        decoder
          .decode::<TokioRuntime>(BytesMut::from(Bytes::from(payload)))
          .await?
      } else {
        decoder
          .decode_from_reader::<_, TokioRuntime>(&mut futures::io::Cursor::new(payload))
          .await?
      };
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

macro_rules! all_unit_test {
  ($name:ident($stream:literal) [ $(($id:ident, $addr: ident)), +$(,)? ]) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy_stream_ $stream >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,
          encryption_algo: EncryptionAlgorithm,
          keys: RandomSecretKeys,
          messages: Vec<Message<$id, $addr>>,
        ) -> bool {
          encode_decode_messages(false, checksum_algo, compress_algo, encryption_algo, keys,  messages, Label::try_from("test").unwrap(), true, $stream)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy_stream_ $stream >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,
          encryption_algo: EncryptionAlgorithm,
          keys: RandomSecretKeys,
          messages: Vec<Message<$id, $addr>>
        ) -> bool {
          encode_decode_messages(false, checksum_algo, compress_algo, encryption_algo, keys,  messages, Label::EMPTY.clone(), false, $stream)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy_stream_ $stream >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,
          encryption_algo: EncryptionAlgorithm,
          keys: RandomSecretKeys,
          message: Message<$id, $addr>,
        ) -> bool {
          encode_decode_message(false, checksum_algo, compress_algo, encryption_algo, keys,  message, Label::try_from("test").unwrap(), true, $stream)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy_stream_ $stream >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,
          encryption_algo: EncryptionAlgorithm,
          keys: RandomSecretKeys,
          message: Message<$id, $addr>,
        ) -> bool {
          encode_decode_message(false, checksum_algo, compress_algo, encryption_algo, keys,  message, Label::EMPTY.clone(), false, $stream)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy_stream_ $stream >](
          messages: Vec<Message<$id, $addr>>,
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,
          encryption_algo: EncryptionAlgorithm,
          keys: RandomSecretKeys,
        ) -> bool {
          encode_decode_messages(true, checksum_algo, compress_algo, encryption_algo, keys,  messages, Label::try_from("test").unwrap(), true, $stream)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy_stream_ $stream >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,
          encryption_algo: EncryptionAlgorithm,
          keys: RandomSecretKeys,
          messages: Vec<Message<$id, $addr>>,
        ) -> bool {
          encode_decode_messages(true, checksum_algo, compress_algo, encryption_algo, keys,  messages, Label::EMPTY.clone(), false, $stream)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy_stream_ $stream >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,
          encryption_algo: EncryptionAlgorithm,
          keys: RandomSecretKeys,
          message: Message<$id, $addr>
        ) -> bool {
          encode_decode_message(true, checksum_algo, compress_algo, encryption_algo, keys,  message, Label::try_from("test").unwrap(), true, $stream)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy_stream_ $stream >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,
          encryption_algo: EncryptionAlgorithm,
          keys: RandomSecretKeys,
          message: Message<$id, $addr>,
        ) -> bool {
          encode_decode_message(true, checksum_algo, compress_algo, encryption_algo, keys,  message, Label::EMPTY.clone(), false, $stream)
        }
      )*
    }
  };
}

all_unit_test!(all(false)[(IpAddr, SocketAddrV4)]);
all_unit_test!(all(true)[(IpAddr, SocketAddrV4)]);
