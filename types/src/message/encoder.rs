use smallvec_wrapper::SmallVec;

use crate::data;

use super::{Data, EncodeError, Label, Message};

const PAYLOAD_LEN_SIZE: usize = 4;
const MAX_MESSAGES_PER_BATCH: usize = 255;
const BATCH_OVERHEAD: usize = 2; // 1 byte for the batch message tag, 1 byte for the number of messages

/// The errors may occur during encoding.

#[derive(Debug, thiserror::Error)]
pub enum EncoderError {
  #[error(transparent)]
  Encode(#[from] EncodeError),
  #[cfg(feature = "encryption")]
  #[error(transparent)]
  Encrypt(#[from] crate::EncryptionError),
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  #[error(transparent)]
  Compress(#[from] crate::CompressionError),
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  #[error(transparent)]
  Checksum(#[from] crate::ChecksumError),
}

/// The hint of how encrypted payload.
#[cfg(feature = "encryption")]
#[derive(Debug, Clone, Copy)]
pub struct EncryptionHint {
  header_offset: usize,
  length_offset: usize,
  nonce_offset: usize,
  nonce_size: usize,
}

#[cfg(feature = "encryption")]
impl EncryptionHint {
  /// - 1 byte for the encryption message tag
  /// - 1 byte for the encryption algorithm
  /// - 4 bytes for the length of the encrypted message
  const HEADER_SIZE: usize = 1 + 1 + PAYLOAD_LEN_SIZE;

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
#[derive(Debug, Clone, Copy)]
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
  const HEADER_SIZE: usize = 1 + 1 + PAYLOAD_LEN_SIZE;

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
#[derive(Debug, Clone, Copy)]
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
  ///
  /// - 1 byte for the compression message tag
  /// - 2 bytes for the compression algorithm
  /// - 4 bytes for the length of the compressed message.
  pub const HEADER_SIZE: usize = 1 + 2 + PAYLOAD_LEN_SIZE;

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

/// The hint for [`MessageEncoder`] to encode the messages.
#[viewit::viewit(vis_all = "", getters(style = "move"), setters(skip))]
#[derive(Clone, Copy, Debug, Default)]
pub struct EncodeHint {
  #[viewit(getter(attrs(doc = "The input size of the messages.",)))]
  input_size: usize,
  #[viewit(getter(attrs(
    doc = "The maximum output size of the messages, this value can be used to pre-allocate the buffer.",
  )))]
  max_output_size: usize,
  #[viewit(getter(attrs(
    doc = "The hint of how the encoder should encode the checksum, `None` if the encoder will not checksum the original payload.",
    cfg(any(
      feature = "zstd",
      feature = "lz4",
      feature = "brotli",
      feature = "snappy",
    ))
  )))]
  checksum_hint: Option<ChecksumHint>,
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

impl EncodeHint {
  const fn new(input_size: usize) -> Self {
    Self {
      input_size,
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
}

/// The encoder of a message
pub struct MessageEncoder<'a, I, A> {
  msgs: &'a [Message<I, A>],
  label: &'a Label,
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  checksum: Option<super::ChecksumAlgorithm>,
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  compress: Option<super::CompressAlgorithm>,
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  min_compression_size: usize,
  #[cfg(feature = "encryption")]
  encrypt: Option<(super::EncryptionAlgorithm, super::SecretKey)>,
  #[allow(dead_code)]
  max_payload_size: usize,
}

impl<'a, I, A> MessageEncoder<'a, I, A> {
  /// Creates a new message encoder.
  #[inline]
  pub const fn new(max_payload_size: usize) -> Self {
    Self {
      msgs: &[],
      label: Label::EMPTY,
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
      min_compression_size: 512,
      #[cfg(feature = "encryption")]
      encrypt: None,
      max_payload_size,
    }
  }

  /// Feeds the encoder with messages.
  pub const fn with_messages(&mut self, msgs: &'a [Message<I, A>]) -> &mut Self {
    self.msgs = msgs;
    self
  }

  /// Feeds the encoder with a label.
  pub const fn with_label(&mut self, label: &'a Label) -> &mut Self {
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
  pub const fn with_checksum(&mut self, checksum: Option<super::ChecksumAlgorithm>) -> &mut Self {
    self.checksum = checksum;
    self
  }

  /// Feeds the encoder with a compression algorithm.
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  pub const fn with_compression(
    &mut self,
    compress: Option<super::CompressAlgorithm>,
  ) -> &mut Self {
    self.compress = compress;
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
  pub const fn with_min_compression_size(&mut self, min_compression_size: usize) -> &mut Self {
    self.min_compression_size = if min_compression_size < 32 {
      32
    } else {
      min_compression_size
    };
    self
  }

  /// Feeds the encoder with an encryption algorithm.
  #[cfg(feature = "encryption")]
  pub const fn with_encryption(
    &mut self,
    encrypt: Option<(super::EncryptionAlgorithm, super::SecretKey)>,
  ) -> &mut Self {
    self.encrypt = encrypt;
    self
  }
}

impl<I, A> MessageEncoder<'_, I, A>
where
  I: Data,
  A: Data,
{
  /// Returns the hint of how to encoder handles the messages.
  pub fn hint(&self, input_size: usize) -> Result<EncodeHint, EncoderError> {
    if input_size == 0 {
      return Ok(EncodeHint::default());
    }

    let mut hint = EncodeHint::new(input_size);

    let mut offset = 0;

    // Now let's check if we need to add a label
    if !self.label.is_empty() {
      let written = self.label.encoded_overhead() + self.label.len();
      hint.max_output_size += written;
      offset += written;
    }

    #[cfg(feature = "encryption")]
    if let Some((algo, _)) = &self.encrypt {
      hint.max_output_size += algo.encrypt_overhead();
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
      if hint.max_output_size >= self.min_compression_size {
        let max_compressed_output_size = compress.max_compress_len(hint.input_size)?;
        hint.max_output_size += CompressHint::HEADER_SIZE + max_compressed_output_size;
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
  pub fn encode(&self) -> Result<Vec<u8>, EncoderError> {
    if self.msgs.len() > 1 {
      self.encode_batch()
    } else {
      self.encode_single()
    }
  }

  fn encode_single(&self) -> Result<Vec<u8>, EncoderError> {
    if self.msgs.is_empty() {
      return Ok(Vec::new());
    }

    // 1. check if we need to compress the payload
    // 2. check if we need to add a checksum
    // 3. check if we need to encrypt the payload
    // 4. check if we need to add a label
    let msg = &self.msgs[0];
    let encoded_len = msg.encoded_len();
    let hint = self.hint(encoded_len)?;

    // Now the encoded length is large enough for encoding/encrypting/compressing/labling
    let mut buf = vec![0u8; hint.max_output_size];
    let mut offset = 0;

    // Do the encoding/encrypting/compressing/labling
    // 1. add the label
    // 2. write the encryption overhead
    // 3. encode the message after the encryption overhead
    let label_size = self.label.len();
    if label_size > 0 {
      buf[0] = super::LABELED_MESSAGE_TAG;
      buf[1] = label_size as u8; // label length can never be larger than 253
      buf[2..2 + label_size].copy_from_slice(self.label.as_bytes());
      offset += 2 + label_size;
    }

    let mut bytes_written;
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

          let written = msg.encode(&mut encoded_buf)?;
          #[cfg(debug_assertions)]
          {
            super::debug_assert_write_eq(written, encoded_len);
            assert_eq!(encoded_len, hint.input_size(), "the actual encoded length {} does not match the encoded length {} in hint", encoded_len, hint.input_size());
          }

          let mut co = ch.header_offset();
          buf[co] = super::COMPRESSED_MESSAGE_TAG; // Add the compression message tag
          co += 1;
          buf[co..co + 2].copy_from_slice(&algo.encode_to_u16().to_be_bytes()); // Add the compression algorithm
          co += 2;

          // Add the original length of the compressed message, this is
          // useful for pre-allocating the buffer when decompressing
          buf[co..co + PAYLOAD_LEN_SIZE].copy_from_slice(&(written as u32).to_be_bytes());
          co += PAYLOAD_LEN_SIZE; // Add the length of the total compressed message

          #[cfg(debug_assertions)]
          assert_eq!(co, ch.payload_offset(), "the actual compress payload offset {} does not match the compress payload offset {} in hint", co, ch.payload_offset());

          // compress to the buffer
          let compressed_len = algo.compress_to(&encoded_buf, &mut buf[co..])?;

          #[cfg(debug_assertions)]
          debug_assert!(compressed_len <= ch.max_output_size(), "compress algo: {algo}, compressed_len: {}, max_compressed_output_size: {}", compressed_len, ch.max_output_size());

          bytes_written = ChecksumHint::HEADER_SIZE + compressed_len;
        } else {
          bytes_written = msg.encode(&mut buf[offset..])?;
          #[cfg(debug_assertions)]
          super::debug_assert_write_eq(bytes_written, encoded_len);
        }
      } else {
        bytes_written = msg.encode(&mut buf[offset..])?;
        #[cfg(debug_assertions)]
        super::debug_assert_write_eq(bytes_written, encoded_len);
      }
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
          let compressed_payload_end = po + bytes_written;
          let checksumed = algo.checksum(&buf[po..compressed_payload_end])?;
          let mut co = ch.header_offset();
          buf[co] = super::CHECKSUMED_MESSAGE_TAG; // Add the checksum message tag
          co += 1;
          buf[co] = algo.as_u8(); // Add the checksum algorithm
          co += 1;
          buf[co..co + PAYLOAD_LEN_SIZE].copy_from_slice(&(bytes_written as u32).to_be_bytes());
          let checksumed_bytes = checksumed.to_be_bytes();
          let output_size = ch.size();
          buf[compressed_payload_end..compressed_payload_end + output_size].copy_from_slice(&checksumed_bytes[..output_size]);
          bytes_written += ChecksumHint::HEADER_SIZE + output_size;
        } else {
          // we do not need to add a checksum, do nothing
        }
      } else {
        // we do not need to add a checksum, do nothing
      }
    }

    #[cfg(feature = "encryption")]
    if let Some(eh) = &hint.encrypted_hint {
      let (algo, pk) = self
        .encrypt
        .expect("when encrypted hint is set, the encryption algorithm must be set");
      let mut eo = eh.header_offset();
      #[cfg(debug_assertions)]
      assert_eq!(eo, offset, "the actual encryption header offset {} does not match the encryption header offset {} in hint", eo, offset);
      buf[eo] = super::ENCRYPTED_MESSAGE_TAG; // Add the encryption message tag
      eo += 1;
      buf[eo] = algo.as_u8(); // Add the encryption algorithm
      eo += 1;
      buf[eo..eo + PAYLOAD_LEN_SIZE].copy_from_slice(&(bytes_written as u32).to_be_bytes());
      eo += PAYLOAD_LEN_SIZE; // 4 bytes to store the encrypted length

      #[cfg(debug_assertions)]
      assert_eq!(
        eo,
        eh.nonce_offset(),
        "the actual nonce offset {} does not match the nonce offset {} in hint",
        eo,
        eh.nonce_offset()
      );

      let nonce = super::EncryptionAlgorithm::random_nonce();
      let nonce_size = eh.nonce_size();
      buf[eo..eo + nonce_size].copy_from_slice(&nonce);
      let suffix_len = algo.encrypted_suffix_len(bytes_written);
      algo.encrypt(
        pk,
        nonce,
        self.label.as_bytes(),
        &mut EncryptionBuffer::new(&mut buf[eh.payload_offset()..]),
      )?;
      bytes_written += suffix_len;
    }

    buf.truncate(offset + bytes_written);
    Ok(buf)
  }

  fn encode_batch(&self) -> Result<Vec<u8>, EncoderError> {
    todo!()
  }

  fn batch_encode_hints(&self) {}
}

// 1500 bytes typically can hold the maximum size of a UDP packet
smallvec_wrapper::smallvec_wrapper!(
  EncodeBuffer<T>([T; 1500]);
);

// // makeCompoundMessages takes a list of messages and packs
// // them into one or multiple messages based on the limitations
// // of compound messages (255 messages each).
// func makeCompoundMessages(msgs [][]byte) []*bytes.Buffer {
// 	const maxMsgs = 255
// 	bufs := make([]*bytes.Buffer, 0, (len(msgs)+(maxMsgs-1))/maxMsgs)

// 	for ; len(msgs) > maxMsgs; msgs = msgs[maxMsgs:] {
// 		bufs = append(bufs, makeCompoundMessage(msgs[:maxMsgs]))
// 	}
// 	if len(msgs) > 0 {
// 		bufs = append(bufs, makeCompoundMessage(msgs))
// 	}

// 	return bufs
// }

// // makeCompoundMessage takes a list of messages and generates
// // a single compound message containing all of them
// func makeCompoundMessage(msgs [][]byte) *bytes.Buffer {
// 	// Create a local buffer
// 	buf := bytes.NewBuffer(nil)

// 	// Write out the type
// 	buf.WriteByte(uint8(compoundMsg))

// 	// Write out the number of message
// 	buf.WriteByte(uint8(len(msgs)))

// 	// Add the message lengths
// 	for _, m := range msgs {
// 		binary.Write(buf, binary.BigEndian, uint16(len(m)))
// 	}

// 	// Append the messages
// 	for _, m := range msgs {
// 		buf.Write(m)
// 	}

// 	return buf
// }

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
  fn new(payload_overhead: usize) -> Self {
    Self {
      current_encoded_size: payload_overhead + BATCH_OVERHEAD,
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
  fn start_new_batch(&mut self, payload_overhead: usize, idx: usize, need: usize) {
    self.current_encoded_size = payload_overhead + BATCH_OVERHEAD + need;
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
pub enum BatchHint {
  /// Batch should contains only one [`Message`]
  One {
    /// The index of this message belongs to the original slice
    idx: usize,
    /// The encoded size of this message
    encoded_size: usize,
  },
  /// Batch should contains multiple  [`Message`]s
  More {
    /// The range of this batch belongs to the original slice
    range: core::ops::Range<usize>,
    /// The encoded size of this batch
    encoded_size: usize,
  },
}

/// A batch of messages.
#[derive(Debug, Clone)]
enum Batch<'a, I, A> {
  /// Batch contains only one [`Message`].
  One {
    /// The message in this batch.
    msg: &'a Message<I, A>,
    /// The estimated encoded size of this [`Message`].
    estimate_encoded_size: usize,
  },
  /// Batch contains multiple [`Message`]s.
  More {
    /// The estimated encoded size of this batch.
    estimate_encoded_size: usize,
    /// The messages in this batch.
    msgs: &'a [Message<I, A>],
    /// The num of msgs
    num_msgs: usize,
  },
}

#[derive(Debug)]
enum Either<'a, I, A> {
  A(core::iter::Once<&'a Message<I, A>>),
  B(<&'a [Message<I, A>] as IntoIterator>::IntoIter),
}

impl<'a, I, A> Iterator for Batch<'a, I, A> {
  type Item = Either<'a, I, A>;

  fn next(&mut self) -> Option<Self::Item> {
    match self {
      Self::One { msg, .. } => Some(Either::A(core::iter::once(msg))),
      Self::More { msgs, .. } => Some(Either::B(msgs.iter())),
    }
  }
}

impl<I, A> Batch<'_, I, A> {
  /// Returns the estimated encoded size for this batch.
  #[inline]
  pub const fn estimate_encoded_size(&self) -> usize {
    match self {
      Self::One {
        estimate_encoded_size,
        ..
      } => *estimate_encoded_size,
      Self::More {
        estimate_encoded_size,
        ..
      } => *estimate_encoded_size,
    }
  }

  /// Returns the number of messages in this batch.
  #[inline]
  pub fn len(&self) -> usize {
    match self {
      Self::One { .. } => 1,
      Self::More { num_msgs, .. } => *num_msgs,
    }
  }

  /// Returns `true` if this batch is empty.
  #[inline]
  pub fn is_empty(&self) -> bool {
    match self {
      Self::One { .. } => false,
      Self::More { num_msgs, .. } => *num_msgs == 0,
    }
  }
}

fn batch<I, A>(
  fixed_payload_overhead: usize,
  max_encoded_batch_size: usize,
  max_encoded_message_size: usize,
  msgs: &[Message<I, A>],
) -> impl Iterator<Item = Batch<'_, I, A>> + core::fmt::Debug + '_
where
  I: Data + Send + Sync + 'static,
  A: Data + Send + Sync + 'static,
{
  let hints = batch_hints(
    fixed_payload_overhead,
    max_encoded_batch_size,
    max_encoded_message_size,
    msgs,
  );

  #[cfg(feature = "tracing")]
  tracing::trace!(hints=?hints, "memberslit: batch hints");

  hints.scan(0usize, move |idx, hint| {
    Some(match hint {
      BatchHint::One { encoded_size, .. } => {
        let b = Batch::One {
          msg: &msgs[*idx],
          estimate_encoded_size: encoded_size,
        };
        *idx += 1;
        b
      }
      BatchHint::More {
        range,
        encoded_size,
      } => {
        let num = range.end - range.start;
        let b = Batch::More {
          estimate_encoded_size: encoded_size,
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
#[auto_enums::auto_enum(Iterator, ExactSizeIterator, Debug)]
fn batch_hints<I, A>(
  payload_overhead: usize,
  max_encoded_batch_size: usize,
  max_encoded_message_size: usize,
  msgs: &[Message<I, A>],
) -> impl Iterator<Item = BatchHint> + core::fmt::Debug + '_
where
  I: Data + Send + Sync + 'static,
  A: Data + Send + Sync + 'static,
{
  match msgs.len() {
    0 => core::iter::empty(),
    1 => {
      let msg_encoded_len = msgs[0].encoded_len_with_length_delimited();
      core::iter::once(BatchHint::One {
        idx: 0,
        encoded_size: msg_encoded_len,
      })
    }
    total_len => {
      // let mut state = BatchingState::new(payload_overhead);

      msgs
        .iter()
        .enumerate()
        .scan(
          (None, BatchingState::new(payload_overhead)),
          move |(last_hint, state), (idx, msg)| {
            // Handle any remaining hint from previous iteration
            if let Some(hint) = last_hint.take() {
              return Some(Some(hint));
            }

            let msg_encoded_len = msg.encoded_len_with_length_delimited();

            // Handle oversized messages
            if msg_encoded_len > max_encoded_message_size {
              return Some(Some(BatchHint::One {
                idx,
                encoded_size: payload_overhead + msg_encoded_len,
              }));
            }

            let is_last_message = idx + 1 == total_len;

            if state.would_exceed_limits(msg_encoded_len, max_encoded_batch_size) {
              // Finish current batch
              let current_hint = BatchHint::More {
                range: state.batch_start_idx..idx,
                encoded_size: state.current_encoded_size,
              };

              // Start new batch
              state.start_new_batch(payload_overhead, idx, msg_encoded_len);

              if is_last_message {
                // Store last message hint for next iteration
                *last_hint = Some(BatchHint::One {
                  idx,
                  encoded_size: payload_overhead + msg_encoded_len,
                });
              }

              Some(Some(current_hint))
            } else {
              state.add_to_batch(msg_encoded_len);

              if is_last_message {
                Some(Some(BatchHint::More {
                  range: state.batch_start_idx..idx + 1,
                  encoded_size: state.current_encoded_size,
                }))
              } else {
                Some(None)
              }
            }
          },
        )
        .flatten()
    }
  }
}

#[cfg(feature = "encryption")]
struct EncryptionBuffer<'a> {
  buf: &'a mut [u8],
  len: usize,
}

#[cfg(feature = "encryption")]
const _: () = {
  impl<'a> EncryptionBuffer<'a> {
    #[inline]
    const fn new(buf: &'a mut [u8]) -> Self {
      Self { buf, len: 0 }
    }
  }

  impl AsRef<[u8]> for EncryptionBuffer<'_> {
    fn as_ref(&self) -> &[u8] {
      &self.buf[..self.len]
    }
  }

  impl AsMut<[u8]> for EncryptionBuffer<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
      &mut self.buf[..self.len]
    }
  }

  impl aead::Buffer for EncryptionBuffer<'_> {
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

#[test]
fn test_batch() {
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  let single = Message::<SmolStr, SocketAddr>::UserData("ping".into());
  let encoded_len = single.encoded_len_with_length_delimited();
  let msgs = [single];
  let batches = batch::<_, _>(0, 1400, 1400, &msgs).collect::<Vec<_>>();
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
      total_encoded_len += msg.encoded_len_with_length_delimited();
      msg
    })
    .collect::<SmallVec<Message<SmolStr, SocketAddr>>>();

  let batches = batch::<_, _>(0, 1400, 1400, &bcasts).collect::<Vec<_>>();
  assert_eq!(batches.len(), 2, "bad len {}", batches.len());
  assert_eq!(batches[0].len() + batches[1].len(), 256, "missing packets");
  assert_eq!(
    batches[0].estimate_encoded_size() + batches[1].estimate_encoded_size(),
    total_encoded_len + 2 + 2,
    "bad estimate len"
  );
}

// #[test]
// fn test_batch_large_max_encoded_batch_size() {
//   use smol_str::SmolStr;
//   use std::net::SocketAddr;

//   let mut total_encoded_len = 0;
//   let mut last_one_encoded_len = 0;
//   let bcasts = (0..256)
//     .map(|i| {
//       let msg = Message::UserData(i.to_string().as_bytes().to_vec().into());
//       let encoded_len = Lpe::<_, _>::encoded_len(&msg);
//       if i == 255 {
//         last_one_encoded_len = encoded_len;
//       } else {
//         total_encoded_len += encoded_len;
//       }
//       msg
//     })
//     .collect::<SmallVec<Message<SmolStr, SocketAddr>>>();

//   let batches =
//     batch::<_, _>(0, 6, 4, u32::MAX as usize, u32::MAX as usize, 255, bcasts);
//   assert_eq!(batches.len(), 2, "bad len {}", batches.len());
//   assert_eq!(batches[0].len() + batches[1].len(), 256, "missing packets");
//   assert_eq!(
//     batches[0].estimate_encoded_size(),
//     6 + batches[0].len() * 4 + total_encoded_len,
//     "bad encoded len for batch 0"
//   );
//   assert_eq!(
//     batches[1].estimate_encoded_size(),
//     last_one_encoded_len,
//     "bad encoded len for batch 1"
//   );
//   assert_eq!(
//     batches[0].estimate_encoded_size() + batches[1].estimate_encoded_size(),
//     6 + batches[0].len() * 4 + total_encoded_len + last_one_encoded_len,
//     "bad estimate len"
//   );
// }
