use super::{
  ChecksumAlgorithm, Data, Label, Message, MessagesDecoder, ProtoDecoder, ProtoEncoder,
  ProtoEncoderError,
};

use bytes::{Bytes, BytesMut};
use std::net::{IpAddr, SocketAddrV4};

fn encode_decode_messages<I, A>(
  parallel: bool,
  algo: ChecksumAlgorithm,
  messages: Vec<Message<I, A>>,
  label: Label,
  check_label: bool,
) -> bool
where
  I: Data + PartialEq,
  A: Data + PartialEq,
{
  let res: Result<(), Box<dyn std::error::Error>> = futures::executor::block_on(async move {
    let mut encoder = ProtoEncoder::new(1500);
    encoder
      .with_messages(&messages)
      .with_checksum(algo)
      .with_label(&label);

    let data = {
      cfg_if::cfg_if! {
        if #[cfg(feature = "rayon")] {
          use rayon::iter::ParallelIterator;

          if parallel {
            encoder
              .encode_parallel()
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
      let data = decoder.decode(BytesMut::from(Bytes::from(payload))).await?;
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
  algo: ChecksumAlgorithm,
  message: Message<I, A>,
  label: Label,
  check_label: bool,
) -> bool
where
  I: Data + PartialEq,
  A: Data + PartialEq,
{
  let res: Result<(), Box<dyn std::error::Error>> = futures::executor::block_on(async move {
    let mut encoder = ProtoEncoder::new(1500);
    let messages = [message];
    encoder
      .with_messages(&messages)
      .with_checksum(algo)
      .with_label(&label);

    let data = {
      cfg_if::cfg_if! {
        if #[cfg(feature = "rayon")] {
          use rayon::iter::ParallelIterator;

          if parallel {
            encoder
              .encode_parallel()
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
      let data = decoder.decode(BytesMut::from(Bytes::from(payload))).await?;
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

macro_rules! checksum_unit_test {
  ($name:ident($algo:expr) [ $(($id:ident, $addr: ident)), +$(,)? ]) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(false, $algo, messages, Label::try_from("test").unwrap(), true)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(false, $algo, messages, Label::EMPTY.clone(), false)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_message_with_ $name:snake _decoder_skip_label_check_on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(false, $algo, messages, Label::try_from("test").unwrap(), false)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
          encode_decode_message(false, $algo, message, Label::try_from("test").unwrap(), true)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
          encode_decode_message(false, $algo, message, Label::EMPTY.clone(), false)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _decoder_skip_label_check_on _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
          encode_decode_message(false, $algo, message, Label::try_from("test").unwrap(), false)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_multiple_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(true, $algo, messages, Label::try_from("test").unwrap(), true)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_multiple_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(true, $algo, messages, Label::EMPTY.clone(), false)
        }

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_multiple_message_with_ $name:snake _decoder_skip_label_check_on _ $id:snake _ $addr:snake _fuzzy >](messages: Vec<Message<$id, $addr>>) -> bool {
          encode_decode_messages(true, $algo, messages, Label::try_from("test").unwrap(), false)
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

        #[cfg(feature = "rayon")]
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_parallel_decoder_single_message_with_ $name:snake _decoder_skip_label_check_on _ $id:snake _ $addr:snake _fuzzy >](message: Message<$id, $addr>) -> bool {
          encode_decode_message(true, $algo, message, Label::try_from("test").unwrap(), false)
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
