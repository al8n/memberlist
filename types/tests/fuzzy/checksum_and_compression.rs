use super::{
  ChecksumAlgorithm, CompressAlgorithm, Data, Label, Message, MessagesDecoder, ProtoDecoder,
  ProtoEncoder, ProtoEncoderError,
};

use bytes::{Bytes, BytesMut};
use std::net::{IpAddr, SocketAddrV4};

fn encode_decode_messages<I, A>(
  parallel: bool,
  checksum_algo: ChecksumAlgorithm,
  compress_algo: CompressAlgorithm,
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
      .with_checksum(checksum_algo)
      .with_compression(compress_algo)
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
  checksum_algo: ChecksumAlgorithm,
  compress_algo: CompressAlgorithm,
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
      .with_checksum(checksum_algo)
      .with_compression(compress_algo)
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

macro_rules! checksum_and_compression_unit_test {
  ($name:ident [ $(($id:ident, $addr: ident)), +$(,)? ]) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,

          messages: Vec<Message<$id, $addr>>,
        ) -> bool {
          encode_decode_messages(false, checksum_algo, compress_algo,  messages, Label::try_from("test").unwrap(), true)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,

          messages: Vec<Message<$id, $addr>>
        ) -> bool {
          encode_decode_messages(false, checksum_algo, compress_algo,  messages, Label::EMPTY.clone(), false)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,

          message: Message<$id, $addr>,
        ) -> bool {
          encode_decode_message(false, checksum_algo, compress_algo,  message, Label::try_from("test").unwrap(), true)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,

          message: Message<$id, $addr>,
        ) -> bool {
          encode_decode_message(false, checksum_algo, compress_algo,  message, Label::EMPTY.clone(), false)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parchecksum_and_compressionel_decoder_multiple_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,

        ) -> bool {
          encode_decode_messages(true, checksum_algo, compress_algo,  messages, Label::try_from("test").unwrap(), true)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parchecksum_and_compressionel_decoder_multiple_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,

          messages: Vec<Message<$id, $addr>>,
        ) -> bool {
          encode_decode_messages(true, checksum_algo, compress_algo,  messages, Label::EMPTY.clone(), false)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parchecksum_and_compressionel_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,

          message: Message<$id, $addr>
        ) -> bool {
          encode_decode_message(true, checksum_algo, compress_algo,  message, Label::try_from("test").unwrap(), true)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parchecksum_and_compressionel_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          checksum_algo: ChecksumAlgorithm,
          compress_algo: CompressAlgorithm,

          message: Message<$id, $addr>,
        ) -> bool {
          encode_decode_message(true, checksum_algo, compress_algo,  message, Label::EMPTY.clone(), false)
        }
      )*
    }
  };
}

checksum_and_compression_unit_test!(checksum_and_compression[(IpAddr, SocketAddrV4)]);
