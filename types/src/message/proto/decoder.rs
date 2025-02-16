use bytes::{Buf, Bytes, BytesMut};
use triomphe::Arc;

use crate::{message, DecodeError, Label, ParseLabelError};

#[cfg(feature = "encryption")]
use crate::{EncryptionAlgorithm, EncryptionError, SecretKey};

#[cfg(any(
  feature = "zstd",
  feature = "lz4",
  feature = "brotli",
  feature = "snappy",
))]
use crate::{CompressAlgorithm, CompressionError};

#[cfg(any(
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3",
))]
use crate::{ChecksumAlgorithm, ChecksumError};

use super::PAYLOAD_LEN_SIZE;

mod messages_decoder;

pub use messages_decoder::*;

/// An error that can occur during encoding.
#[derive(Debug, thiserror::Error)]
pub enum ProtoDecoderError {
  /// Decoding error
  #[error(transparent)]
  Decode(#[from] DecodeError),
  /// Returned when failed to parse the label.
  #[error(transparent)]
  Label(#[from] ParseLabelError),
  /// The label does not match the expected label
  #[error("The label {actual} does not match the expected label {expected}")]
  LabelMismatch {
    /// The actual label
    actual: Label,
    /// The expected label
    expected: Label,
  },
  /// The offload task is canceled
  #[error("The offload task is canceled")]
  Offload,

  /// Returned when the encryption feature is disabled.
  #[error(
    "Receive an encrypted message while the encryption feature is disabled on the running node"
  )]
  EncryptionDisabled,

  /// Returned when the checksum feature is disabled.
  #[error(
    "Receive a checksumed message while the checksum feature is disabled on the running node"
  )]
  ChecksumDisabled,

  /// Returned when the compression feature is disabled.
  #[error(
    "Receive a compressed message while the compression feature is disabled on the running node"
  )]
  CompressionDisabled,

  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  /// Encryption error
  #[error(transparent)]
  Encryption(#[from] EncryptionError),
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "snappy",
    feature = "brotli",
  ))]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "zstd",
      feature = "lz4",
      feature = "snappy",
      feature = "brotli",
    )))
  )]
  /// Compression error
  #[error(transparent)]
  Compression(#[from] CompressionError),

  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "crc32",
      feature = "xxhash64",
      feature = "xxhash32",
      feature = "xxhash3",
      feature = "murmur3",
    )))
  )]
  /// Checksum error
  #[error(transparent)]
  Checksum(#[from] ChecksumError),
}

impl ProtoDecoderError {
  /// Creates a new label mismatch error.
  #[inline]
  pub const fn label_mismatch(expected: Label, actual: Label) -> Self {
    Self::LabelMismatch { expected, actual }
  }
}

/// A protocol decoder.
///
/// Unlike the [`ProtoEncoder`](super::ProtoEncoder), the decoder will only check label, decrypt, decompress,
/// and verify the checksum of the message. It will not decode the message itself,
/// if you want to decode messages, use the [`MessagesDecoder::new(payload)`],
/// the `payload` is what the [`ProtoDecoder::decode`](ProtoDecoder::decode) returns.
#[derive(Debug, Clone)]
pub struct ProtoDecoder {
  label: Option<Label>,
  #[cfg(any(
    feature = "encryption",
    feature = "zstd",
    feature = "lz4",
    feature = "snappy",
    feature = "brotli",
  ))]
  offload_size: usize,
  #[cfg(feature = "encryption")]
  encrypt: Option<Arc<[SecretKey]>>,
}

impl Default for ProtoDecoder {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl ProtoDecoder {
  /// Creates a new messages encoder.
  #[inline]
  pub const fn new() -> Self {
    Self {
      label: None,
      #[cfg(any(
        feature = "encryption",
        feature = "zstd",
        feature = "lz4",
        feature = "snappy",
        feature = "brotli",
      ))]
      offload_size: 512,
      #[cfg(feature = "encryption")]
      encrypt: None,
    }
  }

  /// Feeds the encoder with a label.
  #[inline]
  pub fn with_label(&mut self, label: Label) -> &mut Self {
    self.label = Some(label);
    self
  }

  /// Feeds or clears the label.
  #[inline]
  pub fn maybe_label(&mut self, label: Option<Label>) -> &mut Self {
    self.label = label;
    self
  }

  /// Clears the label.
  #[inline]
  pub fn without_label(&mut self) -> &mut Self {
    self.label = None;
    self
  }

  /// Feeds the offload size.
  #[cfg(any(
    feature = "encryption",
    feature = "zstd",
    feature = "lz4",
    feature = "snappy",
    feature = "brotli",
  ))]
  #[inline]
  pub fn with_offload_size(&mut self, size: usize) -> &mut Self {
    self.offload_size = size;
    self
  }

  /// Feeds the encoder with an encryption keys.
  #[cfg(feature = "encryption")]
  pub fn with_encryption(&mut self, encrypt: impl Into<Arc<[SecretKey]>>) -> &mut Self {
    self.encrypt = Some(encrypt.into());
    self
  }

  /// Feeds or clears the encryption keys.
  #[cfg(feature = "encryption")]
  pub fn maybe_encryption(&mut self, encrypt: Option<impl Into<Arc<[SecretKey]>>>) -> &mut Self {
    self.encrypt = encrypt.map(Into::into);
    self
  }

  /// Clears the encryption keys.
  #[cfg(feature = "encryption")]
  pub fn without_encryption(&mut self) -> &mut Self {
    self.encrypt = None;
    self
  }

  /// Decodes the to plain message bytes
  pub async fn decode(&self, mut buf: BytesMut) -> Result<Bytes, ProtoDecoderError> {
    if buf.remaining() == 0 {
      return Err(DecodeError::buffer_underflow().into());
    }

    let tag = buf[0];
    let auth_data = if tag == message::LABELED_MESSAGE_TAG {
      let mut offset = 1;
      let len = buf.len();
      if len < offset + 1 {
        return Err(DecodeError::buffer_underflow().into());
      }

      let label_len = buf[offset] as usize;
      offset += 1;
      if len < offset + label_len {
        return Err(DecodeError::buffer_underflow().into());
      }

      let label = &buf[offset..offset + label_len];
      offset += label_len;
      let label = if let Some(expected_label) = &self.label {
        if label != expected_label.as_bytes() {
          return Err(ProtoDecoderError::label_mismatch(
            expected_label.clone(),
            Label::try_from(label)?,
          ));
        } else {
          expected_label.clone()
        }
      } else {
        Label::try_from(label)?
      };

      buf.advance(offset);
      Some(label)
    } else {
      None
    };

    if buf.remaining() == 0 {
      return Err(DecodeError::buffer_underflow().into());
    }

    let tag = buf[0];
    // clear the offset, as in above we may advance the buffer
    let mut unencrypted_buf = if tag == message::ENCRYPTED_MESSAGE_TAG {
      #[cfg(not(feature = "encryption"))]
      return Err(ProtoDecoderError::EncryptionDisabled);

      #[cfg(feature = "encryption")]
      {
        let (algo, encrypted_payload_len) = {
          let mut offset = 1;

          if buf.remaining() < super::ENCRYPTED_MESSAGE_HEADER_SIZE {
            return Err(DecodeError::buffer_underflow().into());
          }
          let algo = EncryptionAlgorithm::from(buf[offset]);
          if algo.is_unknown() {
            return Err(EncryptionError::UnknownAlgorithm(algo).into());
          }
          offset += 1;

          // This len does not contains the encryption algo, nonce, and suffix, only the encrypted payload
          let encrypted_payload_len = buf[offset..offset + PAYLOAD_LEN_SIZE]
            .try_into()
            .map(u32::from_be_bytes)
            .unwrap() as usize;

          buf.advance(super::ENCRYPTED_MESSAGE_HEADER_SIZE);
          (algo, encrypted_payload_len)
        };

        let remaining = buf.remaining();
        let nonce_size = algo.nonce_size();
        let suffix_len = algo.encrypted_suffix_len(encrypted_payload_len);

        if remaining < nonce_size + suffix_len + encrypted_payload_len {
          return Err(DecodeError::buffer_underflow().into());
        }

        #[cfg(feature = "rayon")]
        if encrypted_payload_len > self.offload_size {
          return self
            .clone()
            .offload_decrypt(
              if auth_data.is_some() {
                auth_data
              } else {
                self.label.clone()
              },
              algo,
              nonce_size,
              encrypted_payload_len,
              suffix_len,
              buf,
            )
            .await;
        }

        let nonce = buf.split_to(nonce_size);

        match &self.encrypt {
          None => return Err(EncryptionError::SecretKeyNotFound.into()),
          Some(keys) => {
            if keys.is_empty() {
              return Err(EncryptionError::SecretKeyNotFound.into());
            }

            buf.truncate(encrypted_payload_len + suffix_len);
            let mut success = false;
            for key in keys.iter() {
              let res = algo.decrypt(
                key,
                &nonce,
                auth_data
                  .as_ref()
                  .unwrap_or_else(|| self.label.as_ref().unwrap_or(Label::EMPTY))
                  .as_bytes(),
                &mut buf,
              );

              if res.is_ok() {
                success = true;
                break;
              }
            }

            if !success {
              return Err(EncryptionError::NoInstalledKeys.into());
            }

            buf
          }
        }
      }
    } else {
      buf
    };

    if unencrypted_buf.remaining() == 0 {
      return Err(DecodeError::buffer_underflow().into());
    }

    let tag = unencrypted_buf[0];
    let mut payload_without_checksum = if tag == message::CHECKSUMED_MESSAGE_TAG {
      cfg_if::cfg_if! {
        if #[cfg(any(
          feature = "crc32",
          feature = "xxhash32",
          feature = "xxhash64",
          feature = "xxhash3",
          feature = "murmur3",
        ))] {
          let (algo, payload_size, checksum_size) = {
            let mut offset = 1;
            let len = unencrypted_buf.remaining();
            if len < super::CHECKSUMED_MESSAGE_HEADER_SIZE {
              return Err(DecodeError::buffer_underflow().into());
            }

            let algo = ChecksumAlgorithm::from(unencrypted_buf[offset]);
            if algo.is_unknown() {
              return Err(ChecksumError::UnknownAlgorithm(algo).into());
            }
            offset += 1;

            let payload_len = unencrypted_buf[offset..offset + PAYLOAD_LEN_SIZE]
              .try_into()
              .map(u32::from_be_bytes)
              .unwrap() as usize;
            offset += PAYLOAD_LEN_SIZE;

            let checksum_size = algo.output_size();
            if len < offset + payload_len + checksum_size {
              return Err(DecodeError::buffer_underflow().into());
            }

            unencrypted_buf.advance(offset);
            (algo, payload_len, checksum_size)
          };

          let mut payload_with_checksum = unencrypted_buf.split_to(payload_size + checksum_size);
          let cks = match algo.checksum(&payload_with_checksum[..payload_size]) {
            Ok(cks) => cks.to_be_bytes(),
            Err(e) => return Err(e.into()),
          };

          if payload_with_checksum[payload_size..].ne(&cks[..checksum_size]) {
            return Err(ChecksumError::Mismatch.into());
          }

          payload_with_checksum.split_to(payload_size)
        } else {
          return Err(ProtoDecoderError::ChecksumDisabled)
        }
      }
    } else {
      unencrypted_buf
    };

    if payload_without_checksum.remaining() == 0 {
      return Err(DecodeError::buffer_underflow().into());
    }

    let tag = payload_without_checksum[0];
    let uncompressed_payload = if tag == message::COMPRESSED_MESSAGE_TAG {
      cfg_if::cfg_if! {
        if #[cfg(any(
          feature = "zstd",
          feature = "lz4",
          feature = "snappy",
          feature = "brotli",
        ))] {
          let (algo, payload_size) = {
            let mut offset = 1;
            let len = payload_without_checksum.remaining();
            if len < super::COMPRESSED_MESSAGE_HEADER_SIZE {
              return Err(DecodeError::buffer_underflow().into());
            }

            let algo = CompressAlgorithm::from(u16::from_be_bytes(payload_without_checksum[offset..offset + 2].try_into().unwrap()));
            if algo.is_unknown() {
              return Err(CompressionError::UnknownAlgorithm(algo).into());
            }
            offset += 2;

            let uncompressed_payload_len = payload_without_checksum[offset..offset + PAYLOAD_LEN_SIZE]
              .try_into()
              .map(u32::from_be_bytes)
              .unwrap() as usize;
            offset += PAYLOAD_LEN_SIZE;
            payload_without_checksum.advance(offset);

            #[cfg(feature = "rayon")]
            if len > self.offload_size {
              return Self::offload_decompress(
                algo,
                uncompressed_payload_len,
                payload_without_checksum,
              ).await;
            }

            (algo, uncompressed_payload_len)
          };

          let mut uncompressed_payload = BytesMut::zeroed(payload_size);
          match algo.decompress_to(&payload_without_checksum, &mut uncompressed_payload) {
            Ok(_) => uncompressed_payload,
            Err(e) => return Err(e.into()),
          }
        } else {
          return Err(ProtoDecoderError::CompressionDisabled)
        }
      }
    } else {
      payload_without_checksum
    };

    Ok(uncompressed_payload.freeze())
  }

  #[cfg(feature = "encryption")]
  async fn offload_decrypt(
    self,
    auth_data: Option<Label>,
    algo: EncryptionAlgorithm,
    nonce_size: usize,
    encrypted_payload_len: usize,
    suffix_len: usize,
    mut buf: BytesMut,
  ) -> Result<Bytes, ProtoDecoderError> {
    use futures_channel::oneshot;

    macro_rules! send_res_and_ret {
      ($tx:ident <- $expr:expr) => {{
        if $tx.send($expr).is_err() {
          tracing::error!("memberlist.proto.decoder: failed to send offload result back");
        }
        return;
      }};
    }

    let (tx, rx) = oneshot::channel::<Result<BytesMut, ProtoDecoderError>>();
    rayon::spawn(move || {
      let nonce = buf.split_to(nonce_size);

      match &self.encrypt {
        None => send_res_and_ret!(tx <- Err(EncryptionError::SecretKeyNotFound.into())),
        Some(keys) => {
          if keys.is_empty() {
            send_res_and_ret!(tx <- Err(
              EncryptionError::SecretKeyNotFound.into(),
            ))
          }

          let mut success = false;
          buf.truncate(encrypted_payload_len + suffix_len);
          for key in keys.iter() {
            let res = algo.decrypt(
              key,
              &nonce,
              auth_data
                .as_ref()
                .unwrap_or_else(|| self.label.as_ref().unwrap_or(Label::EMPTY))
                .as_bytes(),
              &mut buf,
            );

            if res.is_ok() {
              success = true;
              break;
            }
          }

          if !success {
            send_res_and_ret!(tx <- Err(EncryptionError::NoInstalledKeys.into()))
          }
        }
      }

      // now we have the decrypted payload
      if buf.remaining() == 0 {
        send_res_and_ret!(tx <- Err(DecodeError::buffer_underflow().into()));
      }

      let tag = buf[0];

      let mut payload_without_checksum = if tag == message::CHECKSUMED_MESSAGE_TAG {
        cfg_if::cfg_if! {
          if #[cfg(any(
            feature = "crc32",
            feature = "xxhash32",
            feature = "xxhash64",
            feature = "xxhash3",
            feature = "murmur3",
          ))] {
            let (algo, payload_size, checksum_size) = {
              let mut offset = 1;
              let len = buf.remaining();
              if len < offset + 1 + PAYLOAD_LEN_SIZE {
                send_res_and_ret!(tx <- Err(DecodeError::buffer_underflow().into()));
              }

              let algo = ChecksumAlgorithm::from(buf[offset]);
              if algo.is_unknown() {
                send_res_and_ret!(tx <- Err(ChecksumError::UnknownAlgorithm(algo).into()));
              }
              offset += 1;

              let payload_len = buf[offset..offset + PAYLOAD_LEN_SIZE]
                .try_into()
                .map(u32::from_be_bytes)
                .unwrap() as usize;
              offset += PAYLOAD_LEN_SIZE;

              let checksum_size = algo.output_size();
              if len < offset + payload_len + checksum_size {
                send_res_and_ret!(tx <- Err(DecodeError::buffer_underflow().into()));
              }

              buf.advance(offset);
              (algo, payload_len, checksum_size)
            };

            let mut payload_with_checksum = buf.split_to(payload_size + checksum_size);
            let cks = match algo.checksum(&payload_with_checksum[..payload_size]) {
              Ok(cks) => cks.to_be_bytes(),
              Err(e) => send_res_and_ret!(tx <- Err(e.into())),
            };

            if payload_with_checksum[payload_size..].ne(&cks[..checksum_size]) {
              send_res_and_ret!(tx <- Err(ChecksumError::Mismatch.into()));
            }

            payload_with_checksum.split_to(payload_size)
          } else {
            send_res_and_ret!(tx <- Err(ProtoDecoderError::ChecksumDisabled))
          }
        }
      } else {
        buf
      };

      if payload_without_checksum.remaining() == 0 {
        send_res_and_ret!(tx <- Err(DecodeError::buffer_underflow().into()));
      }

      let tag = payload_without_checksum[0];
      if tag == message::COMPRESSED_MESSAGE_TAG {
        cfg_if::cfg_if! {
          if #[cfg(any(
            feature = "zstd",
            feature = "lz4",
            feature = "snappy",
            feature = "brotli",
          ))] {
            let (algo, payload_size) = {
              let mut offset = 1;
              let len = payload_without_checksum.remaining();
              if len < super::COMPRESSED_MESSAGE_HEADER_SIZE {
                send_res_and_ret!(tx <- Err(DecodeError::buffer_underflow().into()));
              }

              let algo = CompressAlgorithm::from(u16::from_be_bytes(payload_without_checksum[offset..offset + 2].try_into().unwrap()));
              if algo.is_unknown() {
                send_res_and_ret!(tx <- Err(CompressionError::UnknownAlgorithm(algo).into()));
              }
              offset += 2;

              let uncompressed_payload_len = payload_without_checksum[offset..offset + PAYLOAD_LEN_SIZE]
                .try_into()
                .map(u32::from_be_bytes)
                .unwrap() as usize;
              offset += PAYLOAD_LEN_SIZE;
              payload_without_checksum.advance(offset);
              (algo, uncompressed_payload_len)
            };

            let mut uncompressed_payload = BytesMut::zeroed(payload_size);
            match algo.decompress_to(&payload_without_checksum, &mut uncompressed_payload) {
              Ok(_) => send_res_and_ret!(tx <- Ok(uncompressed_payload)),
              Err(e) => send_res_and_ret!(tx <- Err(e.into())),
            }
          } else {
            send_res_and_ret!(tx <- Err(ProtoDecoderError::CompressionDisabled))
          }
        }
      }

      send_res_and_ret!(tx <- Ok(payload_without_checksum));
    });

    match rx.await {
      Ok(res) => res.map(Bytes::from),
      Err(_) => Err(ProtoDecoderError::Offload),
    }
  }

  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "snappy",
    feature = "brotli",
  ))]
  async fn offload_decompress(
    algo: CompressAlgorithm,
    uncompressed_size: usize,
    payload: BytesMut,
  ) -> Result<Bytes, ProtoDecoderError> {
    let (tx, rx) = futures_channel::oneshot::channel();

    rayon::spawn(move || {
      let mut uncompressed_payload = BytesMut::zeroed(uncompressed_size);
      if tx
        .send(
          algo
            .decompress_to(&payload, &mut uncompressed_payload)
            .map(|_| uncompressed_payload)
            .map_err(Into::into),
        )
        .is_err()
      {
        tracing::error!("memberlist.proto.decoder: failed to send offload result back");
      }
    });

    match rx.await {
      Ok(res) => res.map(Bytes::from),
      Err(_) => Err(ProtoDecoderError::Offload),
    }
  }
}
