use core::marker::PhantomData;

use smallvec_wrapper::SmallVec;

use crate::{
  message::{debug_assert_write_eq, LABELED_MESSAGE_TAG},
  Data, EncodeError, Label, Message, Payload,
};

#[cfg(feature = "encryption")]
use crate::{
  message::{proto::AeadBuffer, ENCRYPTED_MESSAGE_TAG},
  EncryptionAlgorithm, EncryptionError, SecretKey,
};

#[cfg(any(
  feature = "zstd",
  feature = "lz4",
  feature = "brotli",
  feature = "snappy",
))]
use crate::{message::COMPRESSED_MESSAGE_TAG, CompressAlgorithm, CompressionError};

#[cfg(any(
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3",
))]
use crate::{message::CHECKSUMED_MESSAGE_TAG, ChecksumAlgorithm, ChecksumError};

use super::{BATCH_OVERHEAD, MAX_MESSAGES_PER_BATCH, PAYLOAD_LEN_SIZE};

mod blocking_impl;
#[cfg(feature = "rayon")]
mod rayon_impl;

// 1500 bytes typically can hold the maximum size of a UDP packet
smallvec_wrapper::smallvec_wrapper!(
  EncodeBuffer<T>([T; 1500]);
);

/// The errors may occur during encoding.
#[derive(Debug, thiserror::Error)]
pub enum ProtoEncoderError {
  #[error(transparent)]
  Encode(#[from] EncodeError),
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  #[error(transparent)]
  Encrypt(#[from] EncryptionError),
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  #[cfg_attr(
    docsrs,
    feature(any(
      feature = "zstd",
      feature = "lz4",
      feature = "brotli",
      feature = "snappy",
    ))
  )]
  #[error(transparent)]
  Compress(#[from] crate::CompressionError),
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  #[cfg_attr(
    docsrs,
    feature(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3",
    ))
  )]
  #[error(transparent)]
  Checksum(#[from] crate::ChecksumError),
}

/// The hint of how encrypted payload.
#[cfg(feature = "encryption")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncryptionHint {
  header_offset: usize,
  length_offset: usize,
  nonce_offset: usize,
  nonce_size: usize,
}

#[cfg(feature = "encryption")]
impl EncryptionHint {
  const HEADER_SIZE: usize = super::ENCRYPTED_MESSAGE_HEADER_SIZE;

  #[inline]
  const fn new() -> Self {
    Self {
      header_offset: 0,
      length_offset: 0,
      nonce_offset: 0,
      nonce_size: 0,
    }
  }

  /// Returns the start offset of the header.
  #[inline]
  pub const fn header_offset(&self) -> usize {
    self.header_offset
  }

  /// Returns the start offset of the nonce.
  #[inline]
  pub const fn nonce_offset(&self) -> usize {
    self.nonce_offset
  }

  /// Returns the size of the nonce.
  #[inline]
  pub const fn nonce_size(&self) -> usize {
    self.nonce_size
  }

  /// Returns the start offset of the payload.
  #[inline]
  pub const fn payload_offset(&self) -> usize {
    self.nonce_offset + self.nonce_size
  }

  #[inline]
  const fn with_header_offset(mut self, offset: usize) -> Self {
    self.header_offset = offset;
    self
  }

  #[inline]
  const fn with_nonce(mut self, offset: usize, size: usize) -> Self {
    self.nonce_offset = offset;
    self.nonce_size = size;
    self
  }
}

/// The hint of how checksum is encoded to the buffer.
#[cfg(any(
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3",
))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChecksumHint {
  header_offset: usize,
  size: usize,
}

#[cfg(any(
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3",
))]
impl ChecksumHint {
  // - 1 byte for the checksum message tag
  // - 1 byte for the checksum algorithm
  // - 4 bytes for the length of the checksumed message
  const HEADER_SIZE: usize = super::CHECKSUMED_MESSAGE_HEADER_SIZE;

  #[inline]
  const fn new() -> Self {
    Self {
      header_offset: 0,
      size: 0,
    }
  }

  /// Returns the start offset of the header.
  #[inline]
  pub const fn header_offset(&self) -> usize {
    self.header_offset
  }

  /// Returns the payload offset.
  #[inline]
  pub const fn payload_offset(&self) -> usize {
    self.header_offset + Self::HEADER_SIZE
  }

  /// Returns the checksum size.
  #[inline]
  pub const fn size(&self) -> usize {
    self.size
  }

  #[inline]
  const fn with_header_offset(mut self, offset: usize) -> Self {
    self.header_offset = offset;
    self
  }

  #[inline]
  const fn with_size(mut self, size: usize) -> Self {
    self.size = size;
    self
  }
}

/// The hint of how the payload is compressed in the buffer.
#[cfg(any(
  feature = "zstd",
  feature = "lz4",
  feature = "brotli",
  feature = "snappy",
))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressHint {
  header_offset: usize,
  max_output_size: usize,
}

#[cfg(any(
  feature = "zstd",
  feature = "lz4",
  feature = "brotli",
  feature = "snappy",
))]
impl CompressHint {
  /// The header size of the compressed payload.
  const HEADER_SIZE: usize = super::COMPRESSED_MESSAGE_HEADER_SIZE;

  #[inline]
  const fn new() -> Self {
    Self {
      header_offset: 0,
      max_output_size: 0,
    }
  }

  /// Returns the start offset of the header.
  #[inline]
  pub const fn header_offset(&self) -> usize {
    self.header_offset
  }

  /// Returns the payload offset.
  #[inline]
  pub const fn payload_offset(&self) -> usize {
    self.header_offset + Self::HEADER_SIZE
  }

  /// Returns the maximum output size of the compressed payload.
  #[inline]
  pub const fn max_output_size(&self) -> usize {
    self.max_output_size
  }

  #[inline]
  const fn with_header_offset(mut self, offset: usize) -> Self {
    self.header_offset = offset;
    self
  }

  #[inline]
  const fn with_max_output_size(mut self, size: usize) -> Self {
    self.max_output_size = size;
    self
  }
}

/// The hint for [`ProtoEncoder`] to encode the messages.
#[viewit::viewit(vis_all = "", getters(style = "move"), setters(skip))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProtoHint {
  #[viewit(getter(attrs(doc = "The input size of the messages.",)))]
  input_size: usize,
  #[viewit(getter(attrs(
    doc = "The maximum output size of the messages, this value can be used to pre-allocate the buffer.",
  )))]
  max_output_size: usize,

  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  #[viewit(getter(attrs(
    doc = "The hint of how the encoder should encode the checksum, `None` if the encoder will not checksum the original payload.",
    cfg(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3",
    ))
  )))]
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  checksum_hint: Option<ChecksumHint>,

  #[cfg(feature = "encryption")]
  #[viewit(getter(attrs(
    doc = "The hint of how the encoder encrypts, `None` if the encoder will not encrypt the original payload.",
    cfg(feature = "encryption")
  )))]
  encrypted_hint: Option<EncryptionHint>,
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  #[viewit(getter(attrs(
    doc = "The hint of how the encoder compresses the payload, `None` if the encoder will not compress the original payload.",
    cfg(any(
      feature = "zstd",
      feature = "lz4",
      feature = "brotli",
      feature = "snappy",
    ))
  )))]
  compress_hint: Option<CompressHint>,
}

impl ProtoHint {
  const fn new(input_size: usize) -> Self {
    Self {
      input_size,
      // TODO(al8n): change this field to Option to make more sense
      max_output_size: input_size,
      #[cfg(any(
        feature = "crc32",
        feature = "xxhash32",
        feature = "xxhash64",
        feature = "xxhash3",
        feature = "murmur3",
      ))]
      checksum_hint: None,
      #[cfg(feature = "encryption")]
      encrypted_hint: None,
      #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "snappy",
      ))]
      compress_hint: None,
    }
  }

  /// Returns `true` if hints the encoder should offload the encoding to `rayon`.
  pub fn should_offload(&self, offload_threshold: usize) -> bool {
    cfg_if::cfg_if! {
      if #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "snappy",
        feature = "encryption",
      ))] {
        #[cfg(any(
          feature = "zstd",
          feature = "lz4",
          feature = "brotli",
          feature = "snappy",
        ))]
        if self.compress_hint.is_some() && self.input_size > offload_threshold {
          return true;
        }

        #[cfg(feature = "encryption")]
        if self.encrypted_hint.is_some() && self.input_size > offload_threshold {
          return true;
        }

        false
      } else {
        false
      }
    }
  }
}

/// The encoder of messages
pub struct ProtoEncoder<I, A, M> {
  msgs: M,
  label: Label,
  overhead: usize,
  max_payload_size: usize,
  encoded_msgs_len: usize,

  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  checksum: Option<crate::ChecksumAlgorithm>,
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  compress: Option<crate::CompressAlgorithm>,
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  compression_threshold: usize,
  #[cfg(feature = "encryption")]
  encrypt: Option<(EncryptionAlgorithm, SecretKey)>,
  _m: PhantomData<(I, A)>,
}

impl<I, A> ProtoEncoder<I, A, &'static [u8]> {
  /// Creates a new messages encoder.
  #[inline]
  pub const fn new(max_payload_size: usize) -> Self {
    Self {
      msgs: &[],
      label: Label::empty(),
      max_payload_size,
      overhead: 0,
      encoded_msgs_len: 0,
      #[cfg(any(
        feature = "crc32",
        feature = "xxhash32",
        feature = "xxhash64",
        feature = "xxhash3",
        feature = "murmur3",
      ))]
      checksum: None,
      #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "snappy",
      ))]
      compress: None,
      #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "snappy",
      ))]
      compression_threshold: 512,
      #[cfg(feature = "encryption")]
      encrypt: None,
      _m: PhantomData,
    }
  }
}

impl<I, A, M> ProtoEncoder<I, A, M> {
  /// Consumes the encoder and returns the messages.
  pub fn into_messages(self) -> M {
    self.msgs
  }

  /// Returns the messages in the encoder.
  pub fn messages(&self) -> &M {
    &self.msgs
  }

  /// Feeds the overhead for this encoder.
  pub fn with_overhead(mut self, overhead: usize) -> Self {
    self.overhead = overhead;
    self
  }

  /// Returns the overhead of the encoder.
  pub fn overhead(&self) -> usize {
    self.overhead
  }

  /// Feeds the encoder with messages.
  pub fn with_messages<NB>(self, msgs: NB) -> ProtoEncoder<I, A, NB>
  where
    I: Data,
    A: Data,
    NB: AsRef<[Message<I, A>]>,
  {
    let mut this = ProtoEncoder::<I, A, _> {
      msgs,
      label: self.label,
      overhead: self.overhead,
      max_payload_size: self.max_payload_size,
      encoded_msgs_len: 0,
      #[cfg(any(
        feature = "crc32",
        feature = "xxhash32",
        feature = "xxhash64",
        feature = "xxhash3",
        feature = "murmur3",
      ))]
      checksum: self.checksum,
      #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "snappy",
      ))]
      compress: self.compress,
      #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "snappy",
      ))]
      compression_threshold: self.compression_threshold,
      #[cfg(feature = "encryption")]
      encrypt: self.encrypt,
      _m: PhantomData,
    };

    this.encoded_msgs_len = this
      .msgs
      .as_ref()
      .iter()
      .map(|msg| msg.encoded_len_with_length_delimited())
      .sum();
    this
  }
}

impl<I, A, M> ProtoEncoder<I, A, M> {
  /// Feeds the encoder with a label.
  pub fn with_label(mut self, label: Label) -> Self {
    self.label = label;
    self
  }

  /// Feeds the encoder with a checksum algorithm.
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  pub const fn with_checksum(mut self, checksum: ChecksumAlgorithm) -> Self {
    self.checksum = Some(checksum);
    self
  }

  /// Feeds or clear the checksum related settings for the encoder.
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  pub const fn maybe_checksum(&mut self, checksum: Option<ChecksumAlgorithm>) -> &mut Self {
    self.checksum = checksum;
    self
  }

  /// Clears the checksum related settings.
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  pub const fn without_checksum(mut self) -> Self {
    self.checksum = None;
    self
  }

  /// Feeds the encoder with a compression algorithm.
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  pub const fn with_compression(mut self, compress: CompressAlgorithm) -> Self {
    self.compress = Some(compress);
    self
  }

  /// Feeds or clear the compression related settings for the encoder.
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  pub const fn maybe_compression(&mut self, compress: Option<CompressAlgorithm>) -> &mut Self {
    self.compress = compress;
    self
  }

  /// Clears the compression related settings.
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  pub const fn without_compression(mut self) -> Self {
    self.compress = None;
    self
  }

  /// Feeds the encoder with a minimum compression size.
  ///
  /// If the payload size is less than this value, the encoder will not compress the payload.
  ///
  /// Default is 512 bytes. The minimum value is 32 bytes.
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  pub const fn with_compression_threshold(mut self, compression_threshold: usize) -> Self {
    self.compression_threshold = if compression_threshold < 32 {
      32
    } else {
      compression_threshold
    };
    self
  }

  /// Feeds the encoder with an encryption algorithm.
  #[cfg(feature = "encryption")]
  pub const fn with_encryption(mut self, algo: EncryptionAlgorithm, key: SecretKey) -> Self {
    self.encrypt = Some((algo, key));
    self
  }

  /// Feeds the encoder with an encryption algorithm.
  #[cfg(feature = "encryption")]
  pub const fn set_encryption(&mut self, algo: EncryptionAlgorithm, key: SecretKey) -> &mut Self {
    self.encrypt = Some((algo, key));
    self
  }

  /// Feeds or clear the encryption related settings for the encoder.
  #[cfg(feature = "encryption")]
  pub const fn maybe_encryption(
    mut self,
    encrypt: Option<(EncryptionAlgorithm, SecretKey)>,
  ) -> Self {
    self.encrypt = encrypt;
    self
  }

  /// Clears the encryption related settings.
  #[cfg(feature = "encryption")]
  pub const fn without_encryption(mut self) -> Self {
    self.encrypt = None;
    self
  }
}

impl<I, A, M> ProtoEncoder<I, A, M>
where
  I: Data,
  A: Data,
  M: AsRef<[Message<I, A>]>,
{
  /// Returns the hint of how the encoder should process the messages.
  pub fn hint(&self) -> Result<ProtoHint, ProtoEncoderError> {
    self.hint_with_size(self.encoded_msgs_len)
  }

  /// Returns the hint of how the encoder should process the input size data.
  pub fn hint_with_size(&self, input_size: usize) -> Result<ProtoHint, ProtoEncoderError> {
    if input_size == 0 {
      return Ok(ProtoHint::default());
    }

    let mut hint = ProtoHint::new(input_size);

    let mut offset = 0;

    // Now let's check if we need to add a label
    if !self.label.is_empty() {
      let written = self.label.encoded_overhead();
      hint.max_output_size += written;
      offset += written;
    }

    #[cfg(feature = "encryption")]
    if let Some((algo, _)) = &self.encrypt {
      hint.max_output_size += super::ENCRYPTED_MESSAGE_HEADER_SIZE + algo.encrypt_overhead(); // message tag + encryption overhead
      let eh = EncryptionHint::new()
        .with_header_offset(offset)
        .with_nonce(offset + EncryptionHint::HEADER_SIZE, algo.nonce_size());
      hint.encrypted_hint = Some(eh);
      offset += EncryptionHint::HEADER_SIZE + algo.nonce_size();
    }

    #[cfg(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3",
    ))]
    if let Some(checksum) = self.checksum {
      hint.max_output_size += ChecksumHint::HEADER_SIZE + checksum.output_size();
      hint.checksum_hint = Some(
        ChecksumHint::new()
          .with_header_offset(offset)
          .with_size(checksum.output_size()),
      );
      offset += ChecksumHint::HEADER_SIZE;
    }

    #[cfg(any(
      feature = "zstd",
      feature = "lz4",
      feature = "brotli",
      feature = "snappy",
    ))]
    if let Some(compress) = &self.compress {
      if hint.max_output_size >= self.compression_threshold {
        let max_compressed_output_size = compress.max_compress_len(hint.input_size)?;
        hint.max_output_size +=
          CompressHint::HEADER_SIZE + (max_compressed_output_size - hint.input_size);
        hint.compress_hint = Some(
          CompressHint::new()
            .with_header_offset(offset)
            .with_max_output_size(max_compressed_output_size),
        );
      }
    }

    Ok(hint)
  }

  /// Encodes the messages.
  #[auto_enums::auto_enum(Iterator, Debug)]
  pub fn encode(
    &self,
  ) -> impl Iterator<Item = Result<Payload, ProtoEncoderError>> + core::fmt::Debug + '_ {
    let msgs = self.msgs.as_ref();
    match msgs.len() {
      0 => core::iter::empty(),
      1 => {
        let msg = &msgs[0];
        let encoded_len = msg.encoded_len();
        if let Err(err) = self.valid() {
          return core::iter::once(Err(err));
        }

        match self.hint_with_size(encoded_len) {
          Ok(hint) => core::iter::once(self.encode_single(msg, hint)),
          Err(err) => core::iter::once(Err(err)),
        }
      }
      _ => {
        if let Err(err) = self.valid() {
          return core::iter::once(Err(err));
        }

        self.encode_batch()
      }
    }
  }

  fn valid(&self) -> Result<(), ProtoEncoderError> {
    #[cfg(any(
      feature = "zstd",
      feature = "lz4",
      feature = "brotli",
      feature = "snappy",
    ))]
    if let Some(ref algo) = self.compress {
      match algo {
        CompressAlgorithm::Zstd(_) => {
          #[cfg(not(feature = "zstd"))]
          return Err(ProtoEncoderError::Compress(CompressionError::disabled(
            *algo, "zstd",
          )));
        }
        CompressAlgorithm::Lz4 { .. } => {
          #[cfg(not(feature = "lz4"))]
          return Err(ProtoEncoderError::Compress(CompressionError::disabled(
            *algo, "lz4",
          )));
        }
        CompressAlgorithm::Brotli(_) => {
          #[cfg(not(feature = "brotli"))]
          return Err(ProtoEncoderError::Compress(CompressionError::disabled(
            *algo, "brotli",
          )));
        }
        CompressAlgorithm::Snappy => {
          #[cfg(not(feature = "snappy"))]
          return Err(ProtoEncoderError::Compress(CompressionError::disabled(
            *algo, "snappy",
          )));
        }
        CompressAlgorithm::Unknown(_) => {
          return Err(ProtoEncoderError::Compress(
            CompressionError::UnknownAlgorithm(*algo),
          ));
        }
      }
    }

    #[cfg(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3",
    ))]
    if let Some(ref algo) = self.checksum {
      match algo {
        ChecksumAlgorithm::Crc32 => {
          #[cfg(not(feature = "crc32"))]
          return Err(ProtoEncoderError::Checksum(ChecksumError::disabled(
            *algo, "crc32",
          )));
        }
        ChecksumAlgorithm::XxHash32 => {
          #[cfg(not(feature = "xxhash32"))]
          return Err(ProtoEncoderError::Checksum(ChecksumError::disabled(
            *algo, "xxhash32",
          )));
        }
        ChecksumAlgorithm::XxHash64 => {
          #[cfg(not(feature = "xxhash64"))]
          return Err(ProtoEncoderError::Checksum(ChecksumError::disabled(
            *algo, "xxhash64",
          )));
        }
        ChecksumAlgorithm::XxHash3 => {
          #[cfg(not(feature = "xxhash3"))]
          return Err(ProtoEncoderError::Checksum(ChecksumError::disabled(
            *algo, "xxhash3",
          )));
        }
        ChecksumAlgorithm::Murmur3 => {
          #[cfg(not(feature = "murmur3"))]
          return Err(ProtoEncoderError::Checksum(ChecksumError::disabled(
            *algo, "murmur3",
          )));
        }
        ChecksumAlgorithm::Unknown(_) => {
          return Err(ProtoEncoderError::Checksum(
            ChecksumError::UnknownAlgorithm(*algo),
          ));
        }
      }
    }

    Ok(())
  }

  fn encode_single(
    &self,
    msg: &Message<I, A>,
    hint: ProtoHint,
  ) -> Result<Payload, ProtoEncoderError> {
    self.encode_helper(msg, hint)
  }

  fn encode_batch(
    &self,
  ) -> impl Iterator<Item = Result<Payload, ProtoEncoderError>> + core::fmt::Debug + '_ {
    self.batch().map(|batch| match batch {
      Batch::One { msg, hint } => self.encode_single(msg, hint),
      Batch::More {
        msgs,
        hint,
        num_msgs,
      } => {
        let mut buf = EncodeBuffer::with_capacity(hint.input_size);
        buf.resize(hint.input_size, 0);
        buf[0] = super::super::COMPOOUND_MESSAGE_TAG;
        buf[1] = num_msgs as u8;
        let res = match msgs.iter().take(num_msgs).try_fold(
          (super::BATCH_OVERHEAD, &mut buf),
          |(mut offset, buf), msg| match msg.encodable_encode(&mut buf[offset..]) {
            Ok(written) => {
              offset += written;
              Ok((offset, buf))
            }
            Err(err) => Err(err),
          },
        ) {
          Ok((final_size, buf)) => {
            #[cfg(debug_assertions)]
            assert_eq!(
              final_size, hint.input_size,
              "the actual encoded length {} does not match the encoded length {} in hint",
              final_size, hint.input_size
            );
            buf[2..super::BATCH_OVERHEAD]
              .copy_from_slice(&((final_size - super::BATCH_OVERHEAD) as u32).to_be_bytes());
            self.encode_helper(&buf, hint)
          }
          Err(err) => Err(err.into()),
        };

        res
      }
    })
  }

  fn encode_helper<E>(&self, msg: &E, hint: ProtoHint) -> Result<Payload, ProtoEncoderError>
  where
    E: Encodable,
  {
    let (encoded_len, hint) = (hint.input_size, hint);
    let mut payload = Payload::new(self.overhead, hint.max_output_size);
    let buf = payload.data_mut();
    let mut offset = 0;

    let label_size = self.label.len();
    if label_size > 0 {
      buf[0] = LABELED_MESSAGE_TAG;
      buf[1] = label_size as u8; // label length can never be larger than 253
      buf[2..2 + label_size].copy_from_slice(self.label.as_bytes());
      offset += 2 + label_size;
    }

    let mut bytes_written: Option<usize> = None;
    cfg_if::cfg_if! {
      if #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "snappy",
      ))] {
        if let Some(ch) = &hint.compress_hint {
          let algo = self.compress.expect("when compress hint is set, the compression algorithm must be set");
          let mut encoded_buf = EncodeBuffer::new();
          encoded_buf.resize(encoded_len, 0);

          let written = msg.encodable_encode(&mut encoded_buf)?;
          #[cfg(debug_assertions)]
          {
            debug_assert_write_eq(written, encoded_len);
            assert_eq!(encoded_len, hint.input_size(), "the actual encoded length {} does not match the encoded length {} in hint", encoded_len, hint.input_size());
          }

          let mut co = ch.header_offset();
          buf[co] = COMPRESSED_MESSAGE_TAG; // Add the compression message tag
          co += 1;
          buf[co..co + 2].copy_from_slice(&algo.encode_to_u16().to_be_bytes()); // Add the compression algorithm
          co += 2;

          // Add the original length of the compressed message, this is
          // useful for pre-allocating the buffer when decompressing
          buf[co..co + PAYLOAD_LEN_SIZE].copy_from_slice(&(written as u32).to_be_bytes());
          co += PAYLOAD_LEN_SIZE;
          // Reserve the space for the compressed payload length
          co += PAYLOAD_LEN_SIZE;

          let po = ch.payload_offset();
          #[cfg(debug_assertions)]
          assert_eq!(co, po, "the actual compress payload offset {} does not match the compress payload offset {} in hint", co, po);

          // compress to the buffer
          let compressed_len = algo.compress_to(&encoded_buf, &mut buf[po..])?;
          // write the compressed length
          buf[po - PAYLOAD_LEN_SIZE..po].copy_from_slice(&(compressed_len as u32).to_be_bytes());

          #[cfg(debug_assertions)]
          debug_assert!(compressed_len <= ch.max_output_size(), "compress algo: {algo}, compressed_len: {}, max_compressed_output_size: {}, input_size: {}", compressed_len, ch.max_output_size(), hint.input_size);

          bytes_written = Some(CompressHint::HEADER_SIZE + compressed_len);
        }
      } else {}
    }

    cfg_if::cfg_if! {
      if #[cfg(any(
        feature = "crc32",
        feature = "xxhash32",
        feature = "xxhash64",
        feature = "xxhash3",
        feature = "murmur3",
      ))] {
        if let Some(ch) = hint.checksum_hint {
          let algo = self.checksum.expect("when checksum hint is set, the checksum algorithm must be set");
          let po = ch.payload_offset();
          let payload_len = match &mut bytes_written {
            Some(written) => *written,
            // if bytes_written is None, we need to encode the message to the buffer first
            None => {
              let written = msg.encodable_encode(&mut buf[po..])?;
              bytes_written = Some(written);
              written
            },
          };
          let checksum_payload_end = po + payload_len;
          let checksumed = algo.checksum(&buf[po..checksum_payload_end])?;
          let mut co = ch.header_offset();
          buf[co] = CHECKSUMED_MESSAGE_TAG; // Add the checksum message tag
          co += 1;
          buf[co] = algo.as_u8(); // Add the checksum algorithm
          co += 1;
          buf[co..co + PAYLOAD_LEN_SIZE].copy_from_slice(&(payload_len as u32).to_be_bytes());
          let checksumed_bytes = checksumed.to_be_bytes();
          let output_size = ch.size();
          buf[checksum_payload_end..checksum_payload_end + output_size].copy_from_slice(&checksumed_bytes[..output_size]);

          if let Some(written) = &mut bytes_written {
            *written += ChecksumHint::HEADER_SIZE + output_size;
          } else { unreachable!("bytes written cannot be `None`") }
        }
      } else {}
    }

    #[cfg(feature = "encryption")]
    if let Some(eh) = &hint.encrypted_hint {
      let (algo, pk) = self
        .encrypt
        .expect("when encrypted hint is set, the encryption algorithm must be set");
      let mut eo = eh.header_offset();
      #[cfg(debug_assertions)]
      assert_eq!(offset, eo, "the actual encryption header offset {} does not match the encryption header offset {} in hint", offset, eo);
      buf[eo] = ENCRYPTED_MESSAGE_TAG; // Add the encryption message tag
      eo += 1;
      buf[eo] = algo.as_u8(); // Add the encryption algorithm
      eo += 1;

      let po = eh.payload_offset();
      let payload_len = match &mut bytes_written {
        Some(written) => *written,
        None => {
          let written = msg.encodable_encode(&mut buf[po..])?;
          bytes_written = Some(written);
          written
        }
      };

      buf[eo..eo + PAYLOAD_LEN_SIZE].copy_from_slice(&(payload_len as u32).to_be_bytes());
      eo += PAYLOAD_LEN_SIZE; // 4 bytes to store the encrypted length

      #[cfg(debug_assertions)]
      assert_eq!(
        eo,
        eh.nonce_offset(),
        "the actual nonce offset {} does not match the nonce offset {} in hint",
        eo,
        eh.nonce_offset()
      );

      let nonce = EncryptionAlgorithm::random_nonce();
      let nonce_size = eh.nonce_size();
      buf[eo..eo + nonce_size].copy_from_slice(&nonce);
      let suffix_len = algo.encrypted_suffix_len(payload_len);
      algo.encrypt(
        pk,
        nonce,
        self.label.as_bytes(),
        &mut AeadBuffer::new(&mut buf[po..], payload_len),
      )?;

      if let Some(written) = &mut bytes_written {
        *written += super::ENCRYPTED_MESSAGE_HEADER_SIZE + nonce_size + suffix_len;
      } else {
        unreachable!("bytes written cannot be `None`")
      }
    }

    match bytes_written {
      Some(written) => {
        payload.truncate(offset + written);
      }
      None => {
        let written = msg.encodable_encode(&mut buf[offset..])?;
        #[cfg(debug_assertions)]
        debug_assert_write_eq(written, encoded_len);
        payload.truncate(offset + written);
      }
    }

    Ok(payload)
  }

  fn batch(&self) -> impl Iterator<Item = Batch<'_, I, A>> + core::fmt::Debug + '_ {
    let hints = self.batch_hints();

    #[cfg(feature = "tracing")]
    tracing::trace!(hints=?hints, "memberslit: batch hints");

    let msgs = self.msgs.as_ref();
    hints.scan(0usize, move |idx, hint| {
      Some(match hint {
        BatchHint::One { hint, .. } => {
          let b = Batch::One {
            msg: &msgs[*idx],
            hint,
          };
          *idx += 1;
          b
        }
        BatchHint::More { range, hint } => {
          let num = range.end - range.start;
          let b = Batch::More {
            hint,
            msgs: &msgs[*idx..*idx + num],
            num_msgs: num,
          };
          *idx += num;
          b
        }
      })
    })
  }

  /// Calculate batch hints for a slice of messages.
  #[auto_enums::auto_enum(Iterator, Debug, Clone)]
  fn batch_hints(&self) -> impl Iterator<Item = BatchHint> + core::fmt::Debug + Clone + '_ {
    let msgs = self.msgs.as_ref();
    match msgs.len() {
      0 => core::iter::empty(),
      1 => {
        let msg_encoded_len = msgs[0].encoded_len();
        core::iter::once(BatchHint::One {
          idx: 0,
          hint: self.hint_with_size(msg_encoded_len).unwrap(),
        })
      }
      total_len => {
        msgs
          .iter()
          .enumerate()
          .scan(BatchingState::new(), move |state, (idx, msg)| {
            let msg_encoded_len = msg.encoded_len();
            let is_last_message = idx + 1 == total_len;
            let is_single_message = idx + 1 - state.batch_start_idx == 1;

            let hint = if state.would_exceed_limits(msg_encoded_len, self.max_payload_size) {
              // Current message would exceed limits, finish current batch
              let hint = BatchHint::More {
                range: state.batch_start_idx..idx,
                hint: self.hint_with_size(state.current_encoded_size).unwrap(),
              };
              state.start_new_batch(idx, msg_encoded_len);
              Some(hint)
            } else {
              state.add_to_batch(msg_encoded_len);
              if is_last_message {
                // Last message, emit appropriate batch type
                Some(if is_single_message {
                  BatchHint::One {
                    idx,
                    hint: self.hint_with_size(msg_encoded_len).unwrap(),
                  }
                } else {
                  BatchHint::More {
                    range: state.batch_start_idx..idx + 1,
                    hint: self.hint_with_size(state.current_encoded_size).unwrap(),
                  }
                })
              } else {
                None
              }
            };

            // If we started a new batch and it's the last message, add a final hint
            let final_hint = if state.current_num_packets == 1 && is_last_message {
              Some(BatchHint::One {
                idx,
                hint: self.hint_with_size(msg_encoded_len).unwrap(),
              })
            } else {
              None
            };

            Some(hint.into_iter().chain(final_hint))
          })
          .flatten()
      }
    }
  }
}

/// A message batch processing state
#[derive(Clone, Copy, Debug)]
struct BatchingState {
  current_encoded_size: usize,
  current_num_packets: usize,
  batch_start_idx: usize,
  total_batches: usize,
}

impl BatchingState {
  #[inline]
  fn new() -> Self {
    Self {
      current_encoded_size: BATCH_OVERHEAD,
      current_num_packets: 0,
      batch_start_idx: 0,
      total_batches: 0,
    }
  }

  #[inline]
  fn would_exceed_limits(&self, need: usize, max_size: usize) -> bool {
    need + self.current_encoded_size > max_size
      || self.current_num_packets >= MAX_MESSAGES_PER_BATCH
  }

  #[inline]
  fn start_new_batch(&mut self, idx: usize, need: usize) {
    self.current_encoded_size = BATCH_OVERHEAD + need;
    self.current_num_packets = 1;
    self.batch_start_idx = idx;
    self.total_batches += 1;
  }

  #[inline]
  fn add_to_batch(&mut self, need: usize) {
    self.current_num_packets += 1;
    self.current_encoded_size += need;
  }
}

/// Used to indicate how to batch a collection of messages.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BatchHint {
  /// Batch should contains only one [`Message`]
  One {
    /// The index of this message belongs to the original slice
    idx: usize,
    /// The encoded size of this message
    hint: ProtoHint,
  },
  /// Batch should contains multiple  [`Message`]s
  More {
    /// The range of this batch belongs to the original slice
    range: core::ops::Range<usize>,
    /// The encoded hint of this batch
    hint: ProtoHint,
  },
}

/// A batch of messages.
#[derive(Debug, Clone)]
enum Batch<'a, I, A> {
  /// Batch contains only one [`Message`].
  One {
    /// The message in this batch.
    msg: &'a Message<I, A>,
    /// The encoded hint of this [`Message`].
    hint: ProtoHint,
  },
  /// Batch contains multiple [`Message`]s.
  More {
    /// The encoded hint of this batch of [`Message`]s.
    hint: ProtoHint,
    /// The messages in this batch.
    msgs: &'a [Message<I, A>],
    /// The num of msgs
    num_msgs: usize,
  },
}

trait Encodable {
  fn encodable_encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError>;
}

impl<I, A> Encodable for Message<I, A>
where
  I: Data,
  A: Data,
{
  fn encodable_encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    self.encode(buf)
  }
}

impl<T> Encodable for T
where
  T: AsRef<[u8]>,
{
  fn encodable_encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let len = self.as_ref().len();
    if len > buf.len() {
      return Err(EncodeError::insufficient_buffer(len, buf.len()));
    }
    buf[..len].copy_from_slice(self.as_ref());
    Ok(len)
  }
}

#[cfg(test)]
mod tests {
  use smallvec_wrapper::SmallVec;
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  use super::*;

  impl<I, A> Batch<'_, I, A> {
    fn estimate_encoded_size(&self) -> usize {
      match self {
        Self::One { hint, .. } => hint.max_output_size,
        Self::More { hint, .. } => hint.max_output_size,
      }
    }

    #[inline]
    fn len(&self) -> usize {
      match self {
        Self::One { .. } => 1,
        Self::More { num_msgs, .. } => *num_msgs,
      }
    }
  }

  #[test]
  fn test_batch() {
    let encoder = ProtoEncoder::new(1400);
    let single = Message::<SmolStr, SocketAddr>::UserData("ping".into());
    let encoded_len = single.encoded_len();
    let msgs = [single];
    let encoder = encoder.with_messages(&msgs);

    let batches = encoder.batch().collect::<Vec<_>>();
    assert_eq!(batches.len(), 1, "bad len {}", batches.len());
    assert_eq!(
      batches[0].estimate_encoded_size(),
      encoded_len,
      "bad estimate len"
    );

    let mut total_encoded_len = 0;
    let bcasts = (0..256)
      .map(|i| {
        let msg = Message::UserData(i.to_string().as_bytes().to_vec().into());
        total_encoded_len += msg.encoded_len();
        msg
      })
      .collect::<SmallVec<Message<SmolStr, SocketAddr>>>();

    let encoder = encoder.with_messages(&bcasts);

    let batches = encoder.batch().collect::<Vec<_>>();
    assert_eq!(batches.len(), 2, "bad len {}", batches.len());
    assert_eq!(batches[0].len() + batches[1].len(), 256, "missing packets");
    assert_eq!(batches[0].len(), 255);
    assert_eq!(batches[1].len(), 1);
    assert_eq!(
      batches[0].estimate_encoded_size() + batches[1].estimate_encoded_size(),
      total_encoded_len + BATCH_OVERHEAD, // 2 bytes for the compound message overhead, only the first batch has the overhead
      "bad estimate len"
    );
  }

  #[test]
  fn test_batch_large_max_encoded_batch_size() {
    use smol_str::SmolStr;
    use std::net::SocketAddr;

    let mut total_encoded_len = BATCH_OVERHEAD;
    let mut last_one_encoded_len = 0;
    let bcasts = (0..256)
      .map(|i| {
        let msg = Message::UserData(i.to_string().as_bytes().to_vec().into());
        let encoded_len = msg.encoded_len();
        if i == 255 {
          last_one_encoded_len = encoded_len;
        } else {
          total_encoded_len += encoded_len;
        }
        msg
      })
      .collect::<SmallVec<Message<SmolStr, SocketAddr>>>();

    let encoder = ProtoEncoder::new(u32::MAX as usize).with_messages(&bcasts);

    let batches = encoder.batch().collect::<Vec<_>>();
    assert_eq!(batches.len(), 2, "bad len {}", batches.len());
    assert_eq!(batches[0].len(), 255, "missing packets");
    assert_eq!(batches[1].len(), 1, "missing packets");
    assert_eq!(
      batches[0].estimate_encoded_size(),
      total_encoded_len,
      "bad encoded len for batch 0"
    );
    assert_eq!(
      batches[0].estimate_encoded_size() + batches[1].estimate_encoded_size(),
      total_encoded_len + last_one_encoded_len,
      "bad estimate len"
    );
  }
}
