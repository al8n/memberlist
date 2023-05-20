use std::{f32::consts::E, sync::atomic::Ordering};

use crate::{
  error::Error,
  label::{remove_label_header_from_stream, LabeledConnection},
  security::{append_bytes, encrypted_length, EncryptionAlgo, SecurityError},
  showbiz::ShowbizDelegates,
  util::{compress_payload, decompress_buffer, CompressError},
  Options, SecretKeyring,
};

use super::*;
use bytes::{Buf, BufMut, BytesMut};
use futures_util::{
  future::{BoxFuture, FutureExt},
  io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
};
use showbiz_traits::{
  AliveDelegate, ConflictDelegate, Connection, Delegate, EventDelegate, MergeDelegate,
  PingDelegate, VoidAliveDelegate, VoidConflictDelegate, VoidDelegate, VoidEventDelegate,
  VoidMergeDelegate, VoidPingDelegate,
};
use showbiz_types::{InvalidMessageType, MessageType};

mod packet;
mod stream;

#[derive(Debug, thiserror::Error)]
enum InnerError {
  #[error("{0}")]
  Other(&'static str),
  #[error("{0}")]
  InvalidMessageType(#[from] InvalidMessageType),
  #[error("{0}")]
  IO(#[from] std::io::Error),
  #[error("{0}")]
  Encode(#[from] rmp::encode::ValueWriteError),
  #[error("{0}")]
  Decode(#[from] prost::DecodeError),
  #[error("{0}")]
  Compress(#[from] CompressError),
  #[error("{0}")]
  Security(#[from] SecurityError),
}

async fn encrypt_local_state(
  keyring: &SecretKeyring,
  msg: &[u8],
  label: &[u8],
  algo: EncryptionAlgo,
) -> Result<Bytes, InnerError> {
  let enc_len = encrypted_length(algo, msg.len());
  let meta_size = core::mem::size_of::<u8>() + core::mem::size_of::<u32>();
  let mut buf = BytesMut::with_capacity(meta_size + enc_len);

  // Write the encrypt byte
  buf.put_u8(MessageType::Encrypt as u8);

  // Write the size of the message
  buf.put_u32(msg.len() as u32);

  // Authenticated Data is:
  //
  //   [messageType; byte] [messageLength; uint32] [stream_label; optional]
  //
  let mut ciphertext = buf.split_off(meta_size);
  if label.is_empty() {
    // Write the encrypted cipher text to the buffer
    keyring
      .encrypt_payload(algo, msg, &buf, &mut ciphertext)
      .await
      .map(|_| {
        buf.unsplit(ciphertext);
        buf.freeze()
      })
      .map_err(From::from)
  } else {
    let data_bytes = append_bytes(&buf, label);
    // Write the encrypted cipher text to the buffer
    keyring
      .encrypt_payload(algo, msg, &data_bytes, &mut ciphertext)
      .await
      .map(|_| {
        buf.unsplit(ciphertext);
        buf.freeze()
      })
      .map_err(From::from)
  }
}

async fn decrypt_remote_state<R: AsyncRead + std::marker::Unpin>(
  r: &mut LabeledConnection<R>,
  keyring: &SecretKeyring,
) -> Result<Bytes, Error> {
  let meta_size = core::mem::size_of::<u8>() + core::mem::size_of::<u32>();
  let mut buf = BytesMut::with_capacity(meta_size);
  buf.put_u8(MessageType::Encrypt as u8);
  let mut b = [0u8; core::mem::size_of::<u32>()];
  r.read_exact(&mut b).await?;
  buf.put_slice(&b);

  // Ensure we aren't asked to download too much. This is to guard against
  // an attack vector where a huge amount of state is sent
  let more_bytes = u32::from_be_bytes(b) as usize;
  if more_bytes > MAX_PUSH_STATE_BYTES {
    return Err(Error::LargeRemoteState(more_bytes));
  }

  //Start reporting the size before you cross the limit
  if more_bytes > (0.6 * (MAX_PUSH_STATE_BYTES as f64)).floor() as usize {
    tracing::warn!(
      target = "showbiz",
      "remote state size is {} limit is large: {}",
      more_bytes,
      MAX_PUSH_STATE_BYTES
    );
  }

  // Read in the rest of the payload
  buf.resize(meta_size + more_bytes as usize, 0);
  r.read_exact(&mut buf).await?;

  // Decrypt the cipherText with some authenticated data
  //
  // Authenticated Data is:
  //
  //   [messageType; byte] [messageLength; uint32] [label_data; optional]
  //
  let mut ciphertext = buf.split_off(meta_size);
  if r.label().is_empty() {
    // Decrypt the payload
    keyring
      .decrypt_payload(&mut ciphertext, &buf)
      .await
      .map(|_| {
        buf.unsplit(ciphertext);
        buf.freeze()
      })
      .map_err(From::from)
  } else {
    let data_bytes = append_bytes(&buf, r.label());
    // Decrypt the payload
    keyring
      .decrypt_payload(&mut ciphertext, data_bytes.as_ref())
      .await
      .map(|_| {
        buf.unsplit(ciphertext);
        buf.freeze()
      })
      .map_err(From::from)
  }
}
