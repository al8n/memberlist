use super::{Data, Label, Message, ProtoDecoder, ProtoEncoder};

use std::net::SocketAddrV4;

fn encode_decode_messages<I, A>(
  parallel: bool,
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
    .with_label(label.clone());

  let mut decoder = ProtoDecoder::new();
  if check_label {
    decoder.with_label(label);
  }
  super::encode_decode_roundtrip::<_, _, _, agnostic_lite::tokio::TokioRuntime>(
    parallel, stream, encoder, decoder,
  )
}

macro_rules! unit_test {
  ($name:ident [ $(($id:ident, $addr: ident)), +$(,)? ]) => {
    paste::paste! {
      $(
        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          stream: bool,
          parallel: bool,
        ) -> bool {
          encode_decode_messages(parallel, messages, Label::try_from("test").unwrap(), true, stream)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_multiple_packets_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          messages: Vec<Message<$id, $addr>>,
          stream: bool,
          parallel: bool,
        ) -> bool {
          encode_decode_messages(parallel, messages, Label::EMPTY.clone(), false, stream)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _and_label_on _ $id:snake _ $addr:snake _fuzzy >](
          message: Message<$id, $addr>,
          stream: bool,
          parallel: bool,
        ) -> bool {
          encode_decode_message(parallel, message, Label::try_from("test").unwrap(), true, stream)
        }

        #[quickcheck_macros::quickcheck]
        fn [< proto_encoder_decoder_single_message_with_ $name:snake _on _ $id:snake _ $addr:snake _fuzzy >](
          message: Message<$id, $addr>,
          stream: bool,
          parallel: bool,
        ) -> bool {
          encode_decode_message(parallel, message, Label::EMPTY.clone(), false, stream)
        }
      )*
    }
  };
}

unit_test!(plain [
  (u32, u32), (u32, String), (String, SocketAddrV4), (String, u32)
]);
