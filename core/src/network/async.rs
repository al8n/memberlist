use std::sync::atomic::Ordering;

use crate::{
  checksum::Checksumer,
  delegate::Delegate,
  error::Error,
  security::{append_bytes, EncryptionAlgo, SecretKey, SecretKeyring, SecurityError},
  transport::TransportError,
  types::MessageType,
  Options,
};

use super::*;
use agnostic::Runtime;
use bytes::{Buf, BufMut, BytesMut};
use futures_util::{future::FutureExt, Future, Stream};

mod packet;
mod stream;

#[cfg(any(test, feature = "test"))]
pub(crate) mod tests;
#[cfg(any(test, feature = "test"))]
pub use tests::*;

impl<D, T> Showbiz<T, D>
where
  D: Delegate,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  pub(crate) async fn send_ping_and_wait_for_ack(
    &self,
    target: &NodeId,
    ping: Ping,
    deadline: Duration,
  ) -> Result<bool, Error<T, D>> {
    let Ok(mut conn) = self
      .inner
      .transport
      .dial_timeout(target.addr(), deadline)
      .await
    else {
      // If the node is actually dead we expect this to fail, so we
      // shouldn't spam the logs with it. After this point, errors
      // with the connection are real, unexpected errors and should
      // get propagated up.
      return Ok(false);
    };
    if deadline != Duration::ZERO {
      conn.set_timeout(Some(deadline));
    }

    self
      .raw_send_msg_stream(
        &mut conn,
        self.inner.opts.label.clone(),
        ping.encode::<T::Checksumer>(0, 0, 0),
        target.addr(),
      )
      .await?;

    let encryption_enabled = self.encryption_enabled();
    let (h, data) = Self::read_stream(
      &mut conn,
      self.inner.opts.label.clone(),
      encryption_enabled,
      self.inner.opts.secret_keyring.clone(),
      &self.inner.opts,
      #[cfg(feature = "metrics")]
      &self.inner.opts.metric_labels,
    )
    .await?;

    if h.meta.ty != MessageType::AckResponse {
      return Err(Error::Transport(TransportError::Decode(
        DecodeError::MismatchMessageType {
          expected: MessageType::AckResponse.as_str(),
          got: h.meta.ty.as_str(),
        },
      )));
    }

    let (_, ack) =
      AckResponse::decode_archived::<T::Checksumer>(&data).map_err(TransportError::Decode)?;

    if ack.seq_no != ping.seq_no {
      return Err(Error::Transport(TransportError::Decode(
        DecodeError::MismatchSequenceNumber {
          ping: ping.seq_no,
          ack: ack.seq_no,
        },
      )));
    }

    Ok(true)
  }

  /// Used to initiate a push/pull over a stream with a
  /// remote host.
  pub(crate) async fn send_and_receive_state(
    &self,
    id: &NodeId,
    join: bool,
  ) -> Result<RemoteNodeState, Error<T, D>> {
    // Attempt to connect
    let mut conn = self
      .inner
      .transport
      .dial_timeout(id.addr, self.inner.opts.tcp_timeout)
      .await
      .map_err(Error::transport)?;
    tracing::debug!(target = "showbiz", local_addr = %self.inner.id, peer_addr = %id, "initiating push/pull sync");

    #[cfg(feature = "metrics")]
    {
      incr_tcp_connect_counter(self.inner.opts.metric_labels.iter());
    }

    // Send our state
    self
      .send_local_state(&mut conn, id.addr, join, self.inner.opts.label.clone())
      .await?;

    conn.set_timeout(if self.inner.opts.tcp_timeout == Duration::ZERO {
      None
    } else {
      Some(self.inner.opts.tcp_timeout)
    });

    let encryption_enabled = self.encryption_enabled();
    let (h, data) = Self::read_stream(
      &mut conn,
      self.inner.opts.label.clone(),
      encryption_enabled,
      self.inner.opts.secret_keyring.clone(),
      &self.inner.opts,
      #[cfg(feature = "metrics")]
      &self.inner.opts.metric_labels,
    )
    .await?;

    if h.meta.ty == MessageType::ErrorResponse {
      let err = match ErrorResponse::decode_archived::<T::Checksumer>(&data) {
        Ok((_, err)) => err,
        Err(e) => return Err(TransportError::Decode(e).into()),
      };
      return Err(Error::Peer(err.err.to_string()));
    }

    // Quit if not push/pull
    if h.meta.ty != MessageType::PushPull {
      return Err(Error::Transport(TransportError::Decode(
        DecodeError::MismatchMessageType {
          expected: MessageType::PushPull.as_str(),
          got: h.meta.ty.as_str(),
        },
      )));
    }

    // Read remote state
    self.read_remote_state(data).await.map_err(From::from)
  }

  fn encrypt_local_state(
    primary_key: SecretKey,
    keyring: &SecretKeyring,
    msg: &[u8],
    label: &Label,
    algo: EncryptionAlgo,
  ) -> Result<Bytes, Error<T, D>> {
    let mut buf = algo.header(msg.len());

    // Authenticated Data is:
    //
    //   [messageType; u8] [label length; u8] [reserved2; u8] [reserved3; u8]
    //   [messageLength; u32] [checksum; u32] [stream_label; optional] [encryptionAlgo; u8]
    //
    let mut ciphertext = buf.split_off(ENCODE_HEADER_SIZE);
    if label.is_empty() {
      // Write the encrypted cipher text to the buffer
      keyring
        .encrypt_payload(primary_key, algo, msg, &buf, &mut ciphertext)
        .map(|_| {
          let mut h = <T::Checksumer as Checksumer>::new();
          h.update(&ciphertext);
          buf[ENCODE_META_SIZE + MAX_MESSAGE_SIZE..ENCODE_HEADER_SIZE]
            .copy_from_slice(&h.finalize().to_be_bytes());
          buf.unsplit(ciphertext);
          buf.freeze()
        })
        .map_err(From::from)
    } else {
      let data_bytes = append_bytes(&buf, label.as_bytes());
      // Write the encrypted cipher text to the buffer
      keyring
        .encrypt_payload(primary_key, algo, msg, &data_bytes, &mut ciphertext)
        .map(|_| {
          let mut h = <T::Checksumer as Checksumer>::new();
          h.update(&ciphertext);
          buf[ENCODE_META_SIZE + MAX_MESSAGE_SIZE..ENCODE_HEADER_SIZE]
            .copy_from_slice(&h.finalize().to_be_bytes());
          buf.unsplit(ciphertext);
          buf.freeze()
        })
        .map_err(From::from)
    }
  }

  fn decrypt_remote_state(
    stream_label: &Label,
    header: EncodeHeader,
    mut buf: BytesMut,
    keyring: &SecretKeyring,
  ) -> Result<Bytes, Error<T, D>> {
    let EncodeHeader { meta, len, cks } = header;

    // Decrypt the cipherText with some authenticated data
    //
    // Authenticated Data is:
    //
    //   [messageType; u8] [label length; u8] [reserved2; u8] [reserved3; u8]
    //   [messageLength; u32] [checksum; u32] [stream_label; optional] [encryptionAlgo; u8]
    //

    let mut ciphertext = buf.split_off(ENCODE_HEADER_SIZE + EncryptionAlgo::SIZE);
    if stream_label.is_empty() {
      // Decrypt the payload
      keyring
        .decrypt_payload(&mut ciphertext, &buf)
        .map_err(From::from)
        .and_then(|_| {
          let mut h = <T::Checksumer as Checksumer>::new();
          h.update(&ciphertext);
          if cks != h.finalize() {
            return Err(Error::Transport(TransportError::Decode(
              DecodeError::ChecksumMismatch,
            )));
          }
          Ok(ciphertext.freeze())
        })
    } else {
      let data_bytes = append_bytes(&buf, stream_label.as_bytes());
      // Decrypt the payload
      keyring
        .decrypt_payload(&mut ciphertext, data_bytes.as_ref())
        .map_err(From::from)
        .and_then(|_| {
          let mut h = <T::Checksumer as Checksumer>::new();
          h.update(&ciphertext);
          if cks != h.finalize() {
            return Err(Error::Transport(TransportError::Decode(
              DecodeError::ChecksumMismatch,
            )));
          }
          Ok(ciphertext.freeze())
        })
    }
  }
}

struct EncryptedRemoteStateHeader {
  meta_size: usize,
  buf: BytesMut,
}
