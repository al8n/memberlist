pub use decoder::*;
pub use encoder::*;

mod decoder;
mod encoder;

const PAYLOAD_LEN_SIZE: usize = 4;
const MAX_MESSAGES_PER_BATCH: usize = 255;
/// - 1 byte for the batch message tag
/// - 1 byte for the number of messages
/// - 4 bytes for the total length of the messages in bytes
const BATCH_OVERHEAD: usize = 1 + 1 + PAYLOAD_LEN_SIZE;
/// - 1 byte for the label message tag
/// - 1 byte for the label length
const LABEL_OVERHEAD: usize = 1 + 1;

/// - 1 byte for the message tag
/// - 5 bytes for the max u32 varint encoded size
const MAX_PLAIN_MESSAGE_HEADER_SIZE: usize = 1 + 5;

const ENCRYPTED_MESSAGE_HEADER_SIZE: usize = 1 + 1 + PAYLOAD_LEN_SIZE; // 1 byte for the encryption tag, 1 byte algo, 4 bytes for the length
/// - 1 byte for the compression tag
/// - 2 byte for the algo
/// - 4 bytes for the uncompressed data length
/// - 4 bytes for the compressed data length
const COMPRESSED_MESSAGE_HEADER_SIZE: usize = 1 + 2 + PAYLOAD_LEN_SIZE + PAYLOAD_LEN_SIZE;
const CHECKSUMED_MESSAGE_HEADER_SIZE: usize = 1 + 1 + PAYLOAD_LEN_SIZE; // 1 byte for the checksum tag, 1 byte for the algo, 4 bytes for the checksum

#[cfg(feature = "encryption")]
struct AeadBuffer<'a> {
  buf: &'a mut [u8],
  len: usize,
}

#[cfg(feature = "encryption")]
const _: () = {
  impl<'a> AeadBuffer<'a> {
    #[inline]
    const fn new(buf: &'a mut [u8], len: usize) -> Self {
      Self { buf, len }
    }
  }

  impl AsRef<[u8]> for AeadBuffer<'_> {
    fn as_ref(&self) -> &[u8] {
      &self.buf[..self.len]
    }
  }

  impl AsMut<[u8]> for AeadBuffer<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
      &mut self.buf[..self.len]
    }
  }

  impl aead::Buffer for AeadBuffer<'_> {
    fn extend_from_slice(&mut self, other: &[u8]) -> aead::Result<()> {
      if self.len >= self.buf.len() {
        return Err(aead::Error);
      }

      self.buf[self.len..self.len + other.len()].copy_from_slice(other);
      self.len += other.len();
      Ok(())
    }

    fn truncate(&mut self, len: usize) {
      if len >= self.len {
        return;
      }
      self.buf[len..self.len].fill(0);
      self.len = len;
    }
  }
};

#[cfg(test)]
mod tests {
  use std::net::IpAddr;

  use bytes::{BufMut, Bytes, BytesMut};
  use nodecraft::Node;
  use triomphe::Arc;

  use crate::{
    message::proto::AeadBuffer, Alive, ChecksumAlgorithm, EncryptionAlgorithm, Label, Meta, Nack,
    SecretKey,
  };

  use super::{
    super::{Data, DataRef, Message},
    MessagesDecoder, MessagesDecoderIter, ProtoDecoder, ProtoDecoderError, ProtoEncoder,
    ProtoEncoderError,
  };

  fn run<F>(fut: F) -> F::Output
  where
    F:
      core::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>>,
  {
    tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .unwrap()
      .block_on(fut)
  }

  #[quickcheck_macros::quickcheck]
  fn encode_decode_plain_message(message: Message<IpAddr, IpAddr>) -> bool {
    let res = run(async move {
      let messages = [message];
      let label = Label::try_from("test").unwrap();
      let pk = SecretKey::random_aes128();
      let encoder = ProtoEncoder::new(1500)
        .with_messages(&messages)
        .with_compression(crate::CompressAlgorithm::Lz4)
        .with_encryption(EncryptionAlgorithm::NoPadding, pk)
        .with_label(label.clone())
        .with_checksum(ChecksumAlgorithm::Crc32);
      let data = encoder
        .encode()
        .collect::<Result<Vec<_>, ProtoEncoderError>>()?;

      let mut msgs = Vec::new();
      let mut decoder = ProtoDecoder::default();
      decoder
        .with_label(label)
        .with_encryption(triomphe::Arc::from_iter([pk]));
      for payload in data {
        // println!("payload: {:?}", payload);
        let data = decoder
          .decode::<agnostic_lite::tokio::TokioRuntime>(BytesMut::from(Bytes::from(payload)))
          .await?;

        let decoder = MessagesDecoder::<IpAddr, IpAddr, _>::new(data)?;
        for decoded in decoder.iter() {
          let decoded = decoded?;
          msgs.push(Message::<IpAddr, IpAddr>::from_ref(decoded)?);
        }
      }

      assert_eq!(msgs, messages);

      if msgs != messages {
        return Err("messages do not match".into());
      }

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

  #[test]
  fn t() {
    let res = run(async move {
      let message = Message::Alive(
        Alive::new(
          3218360376,
          Node::new(
            IpAddr::V4("117.49.90.72".parse().unwrap()),
            IpAddr::V4("94.244.218.196".parse().unwrap()),
          ),
        )
        .with_meta(
          Meta::from_static(b"hello world, hello world, hello world, hello world").unwrap(),
        ),
      );
      let messages = [message.clone(), message];
      let label = Label::try_from("test").unwrap();
      // let pk = SecretKey::random_aes128();
      let encoder = ProtoEncoder::new(1500)
        .with_messages(&messages)
        .with_compression(crate::CompressAlgorithm::Brotli(Default::default()))
        .with_compression_threshold(32)
        .with_label(label.clone());
      // .with_encryption(EncryptionAlgorithm::NoPadding, pk)
      // .with_label(&label);
      // .with_checksum(Some(ChecksumAlgorithm::Crc32));
      let data = encoder
        .encode()
        .collect::<Result<Vec<_>, ProtoEncoderError>>()?;

      let mut msgs = Vec::new();
      let mut decoder = ProtoDecoder::default();
      decoder.with_offload_size(u16::MAX as usize)
        .with_label(label);
      for payload in data {
        let data = decoder
          .decode_from_reader::<_, agnostic_lite::tokio::TokioRuntime>(&mut futures::io::Cursor::new(payload))
          .await?;
        let decoder = MessagesDecoder::<IpAddr, IpAddr, _>::new(data)?;
        for decoded in decoder.iter() {
          let decoded = decoded?;
          msgs.push(Message::<IpAddr, IpAddr>::from_ref(decoded)?);
        }
      }

      assert_eq!(msgs, messages);

      if msgs != messages {
        return Err("messages do not match".into());
      }

      Ok(())
    });

    match res {
      Ok(_) => {}
      Err(e) => {
        panic!("{e}");
      }
    }
  }

  #[quickcheck_macros::quickcheck]
  fn encode_decode_plain_messages(messages: Vec<Message<String, IpAddr>>) -> bool {
    let res = run(async move {
      let label = Label::try_from("test").unwrap();
      let encoder = ProtoEncoder::new(1500)
        .with_messages(&messages)
        .with_compression(crate::CompressAlgorithm::Brotli(Default::default()));
      let data = encoder
        .encode()
        .collect::<Result<Vec<_>, ProtoEncoderError>>()?;

      let mut msgs = Vec::new();
      let mut decoder = ProtoDecoder::default();
      // decoder.with_label(label);
      for payload in data {
        let data = decoder
          .decode::<agnostic_lite::tokio::TokioRuntime>(BytesMut::from(Bytes::from(payload)))
          .await?;
        let decoder = MessagesDecoder::<String, IpAddr, _>::new(data)?;
        for decoded in decoder.iter() {
          let decoded = decoded?;
          msgs.push(Message::<String, IpAddr>::from_ref(decoded)?);
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
}
