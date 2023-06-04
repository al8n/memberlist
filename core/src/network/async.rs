use std::sync::atomic::Ordering;

use crate::{
  delegate::Delegate,
  error::Error,
  label::LabeledConnection,
  security::{append_bytes, encrypted_length, EncryptionAlgo, SecurityError},
  showbiz::Spawner,
  transport::Connection,
  types::{InvalidMessageType, MessageType},
  util::{compress_payload, decompress_buffer, CompressionError},
  Options, SecretKeyring,
};

use super::*;
use bytes::{Buf, BufMut, BytesMut};
use futures_util::{
  future::{BoxFuture, FutureExt},
  io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
};

mod packet;
mod stream;

#[derive(Debug, thiserror::Error)]
pub enum NetworkError<T: Transport> {
  #[error("{0}")]
  Transport(T::Error),
  #[error("{0}")]
  IO(#[from] std::io::Error),
  #[error("{0}")]
  Remote(String),
  #[error("{0}")]
  Decode(#[from] DecodeError),
  #[error("fail to decode remote state")]
  Decrypt,
  #[error("expected {expected} message but got {got}")]
  WrongMessageType {
    expected: MessageType,
    got: MessageType,
  },
}

impl<D: Delegate, T: Transport, S: Spawner> Showbiz<D, T, S> {
  /// Used to initiate a push/pull over a stream with a
  /// remote host.
  pub(crate) async fn send_and_receive_state(
    &self,
    id: &NodeId,
    join: bool,
  ) -> Result<RemoteNodeState, Error<D, T>> {
    if id.name.is_empty() && self.inner.opts.require_node_names {
      return Err(Error::MissingNodeName);
    }

    // Attempt to connect
    let conn = self
      .inner
      .transport
      .dial_address_timeout(id, self.inner.opts.tcp_timeout)
      .await
      .map_err(Error::transport)?;
    tracing::debug!(target = "showbiz", "initiating push/pull sync with: {}", id);

    #[cfg(feature = "metrics")]
    {
      incr_tcp_connect_counter(self.inner.metrics_labels.iter());
    }

    // Send our state
    let mut lr = LabeledConnection::new(BufReader::new(conn));
    lr.set_label(self.inner.opts.label.clone());

    let encryption_enabled = self.encryption_enabled().await;
    self
      .send_local_state(&mut lr, id, encryption_enabled, join)
      .await?;

    lr.conn
      .get_mut()
      .set_timeout(if self.inner.opts.tcp_timeout == Duration::ZERO {
        None
      } else {
        Some(self.inner.opts.tcp_timeout)
      });

    let (data, mt) = Self::read_stream(
      &mut lr,
      encryption_enabled,
      self.inner.keyring.as_ref(),
      &self.inner.opts,
    )
    .await?;

    if mt == MessageType::ErrorResponse {
      let err = match data {
        Some(mut d) => match ErrorResponse::decode_len(&mut d) {
          Ok(len) => ErrorResponse::decode_from(d.split_to(len)).map_err(NetworkError::Decode)?,
          Err(e) => return Err(NetworkError::Decode(e).into()),
        },
        None => {
          let len = decode_u32_from_reader(&mut lr)
            .await
            .map(|(x, _)| x as usize)?;
          let mut buf = vec![0; len];
          lr.read_exact(&mut buf).await?;
          ErrorResponse::decode_from(buf.into()).map_err(NetworkError::Decode)?
        }
      };
      return Err(NetworkError::Remote(err.err).into());
    }

    // Quit if not push/pull
    if mt != MessageType::PushPull {
      return Err(
        NetworkError::WrongMessageType {
          expected: MessageType::PushPull,
          got: mt,
        }
        .into(),
      );
    }

    // Read remote state
    self
      .read_remote_state(&mut lr, data, id)
      .await
      .map_err(From::from)
  }

  async fn encrypt_local_state(
    keyring: &SecretKeyring,
    msg: &[u8],
    label: &[u8],
    algo: EncryptionAlgo,
  ) -> Result<Bytes, Error<D, T>> {
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
  ) -> Result<Bytes, Error<D, T>> {
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
    buf.resize(meta_size + more_bytes, 0);
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
}
