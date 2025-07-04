use bytes::{Buf, Bytes, BytesMut};
use smallvec_wrapper::XXXLargeVec;
#[cfg(feature = "encryption")]
use triomphe::Arc;

use crate::{DecodeError, Label};

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

use super::*;

pub use messages_decoder::*;
pub use repeated_decoder::*;

mod messages_decoder;
mod repeated_decoder;

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
  #[cfg(feature = "encryption")]
  verify_incoming: bool,
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
      #[cfg(feature = "encryption")]
      verify_incoming: false,
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

  /// Feeds the decoder with a verification flag.
  ///
  /// When set to `true`, the decoder will verify the incoming message (the incoming message must be encrypted).
  #[cfg(feature = "encryption")]
  #[inline]
  pub fn with_verify_incoming(&mut self, verify: bool) -> &mut Self {
    self.verify_incoming = verify;
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

  /// Decode the message from the reader.
  pub async fn decode_from_reader<R, RT>(&self, reader: &mut R) -> std::io::Result<Bytes>
  where
    R: ProtoReader + Unpin,
    RT: agnostic_lite::RuntimeLite,
  {
    use std::io::{Error, ErrorKind};

    let mut tag_buf = [0; 1];
    reader.peek_exact(&mut tag_buf).await?;

    // Try to read the label
    let auth_data = if tag_buf[0] == LABELED_MESSAGE_TAG {
      if let Some(expected_label) = &self.label {
        let mut label_buf = [0; super::LABEL_OVERHEAD];
        reader.peek_exact(&mut label_buf).await?;
        let label_len = super::LABEL_OVERHEAD + label_buf[1] as usize; // label message tag + label length + label data

        let mut label_buf = XXXLargeVec::with_capacity(label_len);
        label_buf.resize(label_len, 0);
        reader.read_exact(&mut label_buf).await?;

        let label = Label::try_from(&label_buf[super::LABEL_OVERHEAD..])
          .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        if label.ne(expected_label) {
          return Err(Error::new(
            ErrorKind::InvalidData,
            ProtoDecoderError::label_mismatch(expected_label.clone(), label),
          ));
        }
        Some(label)
      } else {
        return Err(Error::new(
          ErrorKind::InvalidData,
          "unexpected double stream label header",
        ));
      }
    } else if let Some(label) = &self.label {
      if !label.is_empty() {
        return Err(ProtoDecoderError::LabelNotFound.into());
      } else {
        None
      }
    } else {
      None
    };

    reader.peek_exact(&mut tag_buf).await?;

    if tag_buf[0] == ENCRYPTED_MESSAGE_TAG {
      #[cfg(not(feature = "encryption"))]
      return Err(Error::new(
        ErrorKind::Other,
        ProtoDecoderError::EncryptionDisabled,
      ));

      #[cfg(feature = "encryption")]
      {
        let mut header = [0u8; super::ENCRYPTED_MESSAGE_HEADER_SIZE];
        reader.read_exact(&mut header).await?;
        let algo = EncryptionAlgorithm::from(header[1]);
        if algo.is_unknown() {
          return Err(Error::new(
            ErrorKind::InvalidData,
            EncryptionError::UnknownAlgorithm(algo),
          ));
        }
        let encrypted_payload_len = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
        let nonce_size = algo.nonce_size();
        let suffix_size = algo.encrypted_suffix_len(encrypted_payload_len);

        let mut buf = BytesMut::zeroed(nonce_size + encrypted_payload_len + suffix_size);
        reader.read_exact(&mut buf).await?;

        if buf.len() > self.offload_size {
          #[cfg(feature = "rayon")]
          return self
            .clone()
            .decrypt_on_rayon(
              if auth_data.is_some() {
                auth_data
              } else {
                self.label.clone()
              },
              algo,
              nonce_size,
              encrypted_payload_len,
              suffix_size,
              buf,
            )
            .await
            .map_err(Into::into);

          #[cfg(not(feature = "rayon"))]
          return self
            .clone()
            .decrypt_on_blocking::<RT>(
              if auth_data.is_some() {
                auth_data
              } else {
                self.label.clone()
              },
              algo,
              nonce_size,
              encrypted_payload_len,
              suffix_size,
              buf,
            )
            .await
            .map_err(Into::into);
        }

        return self
          .decrypt(
            if auth_data.is_some() {
              auth_data
            } else {
              self.label.clone()
            },
            algo,
            nonce_size,
            encrypted_payload_len,
            suffix_size,
            buf,
          )
          .map(BytesMut::freeze)
          .map_err(Into::into);
      }
    }

    #[cfg(feature = "encryption")]
    if self.verify_incoming {
      return Err(Error::new(
        ErrorKind::InvalidData,
        "remote is not encrypted, but the local is set to verify incoming messages",
      ));
    }

    if tag_buf[0] == CHECKSUMED_MESSAGE_TAG {
      #[cfg(not(any(
        feature = "crc32",
        feature = "xxhash32",
        feature = "xxhash64",
        feature = "xxhash3",
        feature = "murmur3",
      )))]
      return Err(Error::new(
        ErrorKind::Other,
        ProtoDecoderError::ChecksumDisabled,
      ));

      #[cfg(any(
        feature = "crc32",
        feature = "xxhash32",
        feature = "xxhash64",
        feature = "xxhash3",
        feature = "murmur3",
      ))]
      {
        let mut header = [0u8; super::CHECKSUMED_MESSAGE_HEADER_SIZE];
        reader.peek_exact(&mut header).await?;
        let algo = ChecksumAlgorithm::from(header[1]);
        if algo.is_unknown() {
          return Err(ProtoDecoderError::from(ChecksumError::UnknownAlgorithm(algo)).into());
        }

        let payload_len = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
        let checksum_size = algo.output_size();

        let mut buf =
          BytesMut::zeroed(super::CHECKSUMED_MESSAGE_HEADER_SIZE + payload_len + checksum_size);
        reader.read_exact(&mut buf).await?;

        buf = Self::dechecksum(buf)?;
        if buf.remaining() == 0 {
          return Err(ProtoDecoderError::from(DecodeError::buffer_underflow()).into());
        }

        let tag = buf[0];
        if tag == COMPRESSED_MESSAGE_TAG {
          #[cfg(not(any(
            feature = "zstd",
            feature = "lz4",
            feature = "snappy",
            feature = "brotli",
          )))]
          return Err(ProtoDecoderError::CompressionDisabled.into());

          #[cfg(any(
            feature = "zstd",
            feature = "lz4",
            feature = "snappy",
            feature = "brotli",
          ))]
          {
            if buf.remaining() > self.offload_size {
              #[cfg(feature = "rayon")]
              return Self::decompress_on_rayon(buf).await.map_err(Into::into);

              #[cfg(not(feature = "rayon"))]
              return Self::decompress_on_blocking::<RT>(buf)
                .await
                .map_err(Into::into);
            }

            return Self::decompress(buf)
              .map(BytesMut::freeze)
              .map_err(Into::into);
          }
        }

        return Ok(buf.freeze());
      }
    }

    if tag_buf[0] == COMPRESSED_MESSAGE_TAG {
      #[cfg(not(any(
        feature = "zstd",
        feature = "lz4",
        feature = "snappy",
        feature = "brotli",
      )))]
      return Err(ProtoDecoderError::CompressionDisabled.into());

      #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "snappy",
        feature = "brotli",
      ))]
      {
        let mut header = [0u8; super::COMPRESSED_MESSAGE_HEADER_SIZE];
        reader.peek_exact(&mut header).await?;
        let algo = CompressAlgorithm::from(u16::from_be_bytes(header[1..3].try_into().unwrap()));
        if algo.is_unknown() {
          return Err(ProtoDecoderError::from(CompressionError::UnknownAlgorithm(algo)).into());
        }

        let compressed_size = u32::from_be_bytes(
          header[super::COMPRESSED_MESSAGE_HEADER_SIZE - super::PAYLOAD_LEN_SIZE..]
            .try_into()
            .unwrap(),
        ) as usize;

        let mut payload = BytesMut::zeroed(super::COMPRESSED_MESSAGE_HEADER_SIZE + compressed_size);
        reader.read_exact(&mut payload).await?;

        if payload.len() > self.offload_size {
          #[cfg(feature = "rayon")]
          return Self::decompress_on_rayon(payload).await.map_err(Into::into);

          #[cfg(not(feature = "rayon"))]
          return Self::decompress_on_blocking::<RT>(payload)
            .await
            .map_err(Into::into);
        }

        return Self::decompress(payload)
          .map(BytesMut::freeze)
          .map_err(Into::into);
      }
    }

    if tag_buf[0] == COMPOOUND_MESSAGE_TAG {
      let mut header = [0u8; super::BATCH_OVERHEAD];
      reader.peek_exact(&mut header).await?;
      let total_len = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;

      let mut buf = BytesMut::zeroed(super::BATCH_OVERHEAD + total_len);
      reader.read_exact(&mut buf).await?;

      return Ok(buf.freeze());
    }

    let mut header = [0u8; super::MAX_PLAIN_MESSAGE_HEADER_SIZE];
    reader.peek(&mut header).await?;
    let (length_delimited_size, total_len) =
      varing::decode_u32_varint(&header[1..]).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
    let mut buf = BytesMut::zeroed(1 + length_delimited_size + total_len as usize);
    reader.read_exact(&mut buf).await?;
    let _ = auth_data;
    Ok(Bytes::from(buf))
  }

  /// Decodes the to plain message bytes
  pub async fn decode<RT>(&self, mut buf: BytesMut) -> Result<Bytes, ProtoDecoderError>
  where
    RT: agnostic_lite::RuntimeLite,
  {
    if buf.remaining() == 0 {
      return Err(DecodeError::buffer_underflow().into());
    }

    let tag = buf[0];
    let auth_data = if tag == LABELED_MESSAGE_TAG {
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
        return Err(ProtoDecoderError::double_label());
      };

      buf.advance(offset);
      Some(label)
    } else if let Some(label) = &self.label {
      if !label.is_empty() {
        return Err(ProtoDecoderError::LabelNotFound);
      } else {
        None
      }
    } else {
      None
    };

    if buf.remaining() == 0 {
      return Err(DecodeError::buffer_underflow().into());
    }

    let tag = buf[0];

    // clear the offset, as in above we may advance the buffer
    let unencrypted_buf = if tag == ENCRYPTED_MESSAGE_TAG {
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

        if encrypted_payload_len > self.offload_size {
          #[cfg(feature = "rayon")]
          return self
            .clone()
            .decrypt_on_rayon(
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

          #[cfg(not(feature = "rayon"))]
          return self
            .clone()
            .decrypt_on_blocking::<RT>(
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

        return self
          .decrypt(
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
          .map(BytesMut::freeze);
      }
    } else {
      buf
    };

    #[cfg(feature = "encryption")]
    if self.verify_incoming {
      return Err(ProtoDecoderError::NotEncrypted);
    }

    if unencrypted_buf.remaining() == 0 {
      return Err(DecodeError::buffer_underflow().into());
    }

    let tag = unencrypted_buf[0];
    let payload_without_checksum = if tag == CHECKSUMED_MESSAGE_TAG {
      #[cfg(checksum)]
      {
        Self::dechecksum(unencrypted_buf)?
      }

      #[cfg(not(checksum))]
      return Err(ProtoDecoderError::ChecksumDisabled);
    } else {
      unencrypted_buf
    };

    if payload_without_checksum.remaining() == 0 {
      return Err(DecodeError::buffer_underflow().into());
    }

    let tag = payload_without_checksum[0];
    if tag == COMPRESSED_MESSAGE_TAG {
      #[cfg(compression)]
      {
        if payload_without_checksum.len() > self.offload_size {
          #[cfg(feature = "rayon")]
          return Self::decompress_on_rayon(payload_without_checksum).await;

          #[cfg(not(feature = "rayon"))]
          return Self::decompress_on_blocking::<RT>(payload_without_checksum)
            .await
            .map_err(Into::into);
        }

        return Self::decompress(payload_without_checksum).map(BytesMut::freeze);
      }

      #[cfg(not(compression))]
      return Err(ProtoDecoderError::CompressionDisabled.into());
    }

    let _ = auth_data;

    Ok(payload_without_checksum.freeze())
  }

  #[cfg(checksum)]
  fn dechecksum(mut buf: BytesMut) -> Result<BytesMut, ProtoDecoderError> {
    if buf.remaining() < super::CHECKSUMED_MESSAGE_HEADER_SIZE {
      return Err(DecodeError::buffer_underflow().into());
    }

    let header = buf.split_to(super::CHECKSUMED_MESSAGE_HEADER_SIZE);

    let algo = ChecksumAlgorithm::from(header[1]);
    if algo.is_unknown() {
      return Err(ChecksumError::UnknownAlgorithm(algo).into());
    }

    let payload_len = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
    let checksum_size = algo.output_size();

    if buf.remaining() < payload_len + checksum_size {
      return Err(DecodeError::buffer_underflow().into());
    }

    let mut payload_with_checksum = buf.split_to(payload_len + checksum_size);
    let cks = match algo.checksum(&payload_with_checksum[..payload_len]) {
      Ok(cks) => cks.to_be_bytes(),
      Err(e) => return Err(e.into()),
    };

    if payload_with_checksum[payload_len..].ne(&cks[..checksum_size]) {
      return Err(ChecksumError::Mismatch.into());
    }

    Ok(payload_with_checksum.split_to(payload_len))
  }

  #[cfg(all(compression, feature = "rayon"))]
  async fn decompress_on_rayon(buf: BytesMut) -> Result<Bytes, ProtoDecoderError> {
    use futures_channel::oneshot;

    let (tx, rx) = oneshot::channel::<Result<BytesMut, ProtoDecoderError>>();
    rayon::spawn(move || {
      if tx.send(Self::decompress(buf)).is_err() {
        #[cfg(feature = "tracing")]
        tracing::error!("memberlist.proto.decoder: failed to send offload result back");
      }
    });

    match rx.await {
      Ok(res) => res.map(Bytes::from),
      Err(_) => Err(ProtoDecoderError::Offload),
    }
  }

  #[cfg(all(compression, not(feature = "rayon")))]
  async fn decompress_on_blocking<RT>(buf: BytesMut) -> Result<Bytes, ProtoDecoderError>
  where
    RT: agnostic_lite::RuntimeLite,
  {
    let res = RT::spawn_blocking(move || Self::decompress(buf)).await;

    match res {
      Ok(res) => res.map(BytesMut::freeze),
      Err(_) => Err(ProtoDecoderError::Offload),
    }
  }

  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "snappy",
    feature = "brotli",
  ))]
  fn decompress(mut buf: BytesMut) -> Result<BytesMut, ProtoDecoderError> {
    if buf.remaining() < super::COMPRESSED_MESSAGE_HEADER_SIZE {
      return Err(DecodeError::buffer_underflow().into());
    }

    let header = buf.split_to(super::COMPRESSED_MESSAGE_HEADER_SIZE);

    let algo = CompressAlgorithm::from(u16::from_be_bytes(header[1..3].try_into().unwrap()));
    if algo.is_unknown() {
      return Err(CompressionError::UnknownAlgorithm(algo).into());
    }

    let uncompressed_size =
      u32::from_be_bytes(header[3..3 + PAYLOAD_LEN_SIZE].try_into().unwrap()) as usize;
    let mut uncompressed_buf = BytesMut::zeroed(uncompressed_size);
    match algo.decompress_to(&buf, &mut uncompressed_buf) {
      Ok(_) => Ok(uncompressed_buf),
      Err(e) => Err(e.into()),
    }
  }

  #[cfg(all(feature = "encryption", feature = "rayon"))]
  async fn decrypt_on_rayon(
    self,
    auth_data: Option<Label>,
    algo: EncryptionAlgorithm,
    nonce_size: usize,
    encrypted_payload_len: usize,
    suffix_len: usize,
    buf: BytesMut,
  ) -> Result<Bytes, ProtoDecoderError> {
    use futures_channel::oneshot;

    let (tx, rx) = oneshot::channel::<Result<BytesMut, ProtoDecoderError>>();
    rayon::spawn(move || {
      if tx
        .send(self.decrypt(
          auth_data,
          algo,
          nonce_size,
          encrypted_payload_len,
          suffix_len,
          buf,
        ))
        .is_err()
      {
        #[cfg(feature = "tracing")]
        tracing::error!("memberlist.proto.decoder: failed to send offload result back");
      }
    });

    match rx.await {
      Ok(res) => res.map(Bytes::from),
      Err(_) => Err(ProtoDecoderError::Offload),
    }
  }

  #[cfg(all(feature = "encryption", not(feature = "rayon")))]
  async fn decrypt_on_blocking<RT>(
    self,
    auth_data: Option<Label>,
    algo: EncryptionAlgorithm,
    nonce_size: usize,
    encrypted_payload_len: usize,
    suffix_len: usize,
    buf: BytesMut,
  ) -> Result<Bytes, ProtoDecoderError>
  where
    RT: agnostic_lite::RuntimeLite,
  {
    let res = RT::spawn_blocking(move || {
      self.decrypt(
        auth_data,
        algo,
        nonce_size,
        encrypted_payload_len,
        suffix_len,
        buf,
      )
    })
    .await;

    match res {
      Ok(res) => res.map(BytesMut::freeze),
      Err(_) => Err(ProtoDecoderError::Offload),
    }
  }

  #[cfg(feature = "encryption")]
  fn decrypt(
    &self,
    auth_data: Option<Label>,
    algo: EncryptionAlgorithm,
    nonce_size: usize,
    encrypted_payload_len: usize,
    suffix_len: usize,
    mut buf: BytesMut,
  ) -> Result<BytesMut, ProtoDecoderError> {
    let nonce = buf.split_to(nonce_size);

    let buf = match &self.encrypt {
      None => return Err(ProtoDecoderError::SecretKeyNotFound),
      Some(keys) => {
        if keys.is_empty() {
          return Err(ProtoDecoderError::SecretKeyNotFound);
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
          return Err(ProtoDecoderError::NoInstalledKeys);
        }

        buf
      }
    };

    // now we have the decrypted payload
    if buf.remaining() == 0 {
      return Err(DecodeError::buffer_underflow().into());
    }

    let tag = buf[0];
    let payload_without_checksum = if tag == CHECKSUMED_MESSAGE_TAG {
      #[cfg(checksum)]
      {
        Self::dechecksum(buf)?
      }

      #[cfg(not(checksum))]
      return Err(ProtoDecoderError::ChecksumDisabled);
    } else {
      buf
    };

    if payload_without_checksum.remaining() == 0 {
      return Err(DecodeError::buffer_underflow().into());
    }

    let tag = payload_without_checksum[0];
    if tag == COMPRESSED_MESSAGE_TAG {
      #[cfg(compression)]
      return Self::decompress(payload_without_checksum);

      #[cfg(not(compression))]
      return Err(ProtoDecoderError::CompressionDisabled);
    }

    Ok(payload_without_checksum)
  }
}
