use std::sync::atomic::Ordering;

use crate::{
  delegate::Delegate,
  error::Error,
  security::{
    append_bytes, encrypted_length, EncryptionAlgo, SecretKey, SecretKeyring, SecurityError,
  },
  transport::{ReliableConnection, TransportError},
  types::MessageType,
  util::{compress_payload, decompress_payload},
  Options,
};

use super::*;
use agnostic::Runtime;
use bytes::{Buf, BufMut, BytesMut};
use futures_util::{future::FutureExt, Future, Stream};

mod packet;
mod stream;

impl<D, T, R> Showbiz<D, T, R>
where
  D: Delegate,
  T: Transport,
  R: Runtime,
  <R::Interval as Stream>::Item: Send,
  <R::Sleep as Future>::Output: Send,
{
  pub(crate) async fn send_ping_and_wait_for_ack(
    &self,
    target: &NodeId,
    ping: Ping,
    deadline: Duration,
  ) -> Result<bool, Error<D, T>> {
    let Ok(mut conn) = self.runner().as_ref().unwrap().transport.dial_timeout(target.addr(), deadline).await else {
      // If the node is actually dead we expect this to fail, so we
      // shouldn't spam the logs with it. After this point, errors
      // with the connection are real, unexpected errors and should
      // get propagated up.
      return Ok(false);
    };
    if deadline != Duration::ZERO {
      conn.set_timeout(Some(deadline));
    }

    let mut out = BytesMut::with_capacity(MessageType::SIZE + ping.encoded_len());
    out.put_u8(MessageType::Ping as u8);
    ping.encode_to(&mut out);

    self
      .raw_send_msg_stream(
        &mut conn,
        self.inner.opts.label.clone(),
        out.freeze(),
        target.addr(),
      )
      .await?;

    let encryption_enabled = self.encryption_enabled();
    let (data, mt) = Self::read_stream(
      &mut conn,
      self.inner.opts.label.clone(),
      encryption_enabled,
      self.inner.keyring.clone(),
      &self.inner.opts,
      #[cfg(feature = "metrics")]
      &self.inner.metrics_labels,
    )
    .await?;

    if mt != MessageType::AckResponse {
      return Err(Error::Transport(TransportError::Decode(
        DecodeError::MismatchMessageType {
          expected: MessageType::AckResponse.as_str(),
          got: mt.as_str(),
        },
      )));
    }

    let ack = match data {
      Some(mut d) => match AckResponse::decode_len(&mut d) {
        Ok(len) => AckResponse::decode_from::<T::Checksumer>(d.split_to(len))
          .map_err(TransportError::Decode)?,
        Err(e) => return Err(TransportError::Decode(e).into()),
      },
      None => {
        let len = conn.read_u32_varint().await.map_err(Error::transport)?;
        let mut buf = vec![0; len];
        conn.read_exact(&mut buf).await.map_err(Error::transport)?;
        AckResponse::decode_from::<T::Checksumer>(buf.into()).map_err(TransportError::Decode)?
      }
    };

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
    name: &Name,
    addr: SocketAddr,
    join: bool,
  ) -> Result<RemoteNodeState, Error<D, T>> {
    // Attempt to connect
    let mut conn = self
      .runner()
      .as_ref()
      .unwrap()
      .transport
      .dial_timeout(addr, self.inner.opts.tcp_timeout)
      .await
      .map_err(Error::transport)?;
    tracing::debug!(
      target = "showbiz",
      "initiating push/pull sync with: {}({})",
      name,
      addr
    );

    #[cfg(feature = "metrics")]
    {
      incr_tcp_connect_counter(self.inner.metrics_labels.iter());
    }

    // Send our state
    self
      .send_local_state(&mut conn, addr, join, self.inner.opts.label.clone())
      .await?;

    conn.set_timeout(if self.inner.opts.tcp_timeout == Duration::ZERO {
      None
    } else {
      Some(self.inner.opts.tcp_timeout)
    });

    let encryption_enabled = self.encryption_enabled();
    let (data, mt) = Self::read_stream(
      &mut conn,
      self.inner.opts.label.clone(),
      encryption_enabled,
      self.inner.keyring.clone(),
      &self.inner.opts,
      #[cfg(feature = "metrics")]
      &self.inner.metrics_labels,
    )
    .await?;

    if mt == MessageType::ErrorResponse {
      let err = match data {
        Some(mut d) => match ErrorResponse::decode_len(&mut d) {
          Ok(len) => ErrorResponse::decode_from(d.split_to(len)).map_err(TransportError::Decode)?,
          Err(e) => return Err(TransportError::Decode(e).into()),
        },
        None => {
          let len = conn.read_u32_varint().await.map_err(Error::transport)?;
          let mut buf = vec![0; len];
          conn.read_exact(&mut buf).await.map_err(Error::transport)?;
          ErrorResponse::decode_from(buf.into()).map_err(TransportError::Decode)?
        }
      };
      return Err(Error::Peer(err.err));
    }

    // Quit if not push/pull
    if mt != MessageType::PushPull {
      return Err(Error::Transport(TransportError::Decode(
        DecodeError::MismatchMessageType {
          expected: MessageType::PushPull.as_str(),
          got: mt.as_str(),
        },
      )));
    }

    // Read remote state
    self
      .read_remote_state(&mut conn, data)
      .await
      .map_err(From::from)
  }

  fn encrypt_local_state(
    primary_key: SecretKey,
    keyring: &SecretKeyring,
    msg: &[u8],
    label: &Label,
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
        .encrypt_payload(primary_key, algo, msg, &buf, &mut ciphertext)
        .map(|_| {
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
          buf.unsplit(ciphertext);
          buf.freeze()
        })
        .map_err(From::from)
    }
  }

  async fn read_encrypt_remote_state(
    r: &mut ReliableConnection<T>,
    #[cfg(feature = "metrics")] metrics_labels: &[metrics::Label],
  ) -> Result<EncryptedRemoteStateHeader, Error<D, T>> {
    // Read in enough to determine message length
    let meta_size = MessageType::SIZE + core::mem::size_of::<u32>();
    let mut buf = BytesMut::with_capacity(meta_size);
    buf.put_u8(MessageType::Encrypt as u8);
    let mut b = [0u8; core::mem::size_of::<u32>()];
    r.read_exact(&mut b).await.map_err(Error::transport)?;
    buf.put_slice(&b);
    // Ensure we aren't asked to download too much. This is to guard against
    // an attack vector where a huge amount of state is sent
    let more_bytes = u32::from_be_bytes(b) as usize;
    #[cfg(feature = "metrics")]
    {
      add_sample_to_remote_size_histogram(more_bytes as f64, metrics_labels.iter());
    }

    if more_bytes > MAX_PUSH_STATE_BYTES {
      return Err(Error::Transport(TransportError::RemoteStateTooLarge(
        more_bytes,
      )));
    }

    // Start reporting the size before you cross the limit
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
    r.read_exact(&mut buf[meta_size..])
      .await
      .map_err(Error::transport)?;

    Ok(EncryptedRemoteStateHeader { meta_size, buf })
  }

  fn decrypt_remote_state(
    stream_label: &Label,
    header: EncryptedRemoteStateHeader,
    keyring: &SecretKeyring,
  ) -> Result<Bytes, Error<D, T>> {
    let EncryptedRemoteStateHeader { meta_size, mut buf } = header;

    // Decrypt the cipherText with some authenticated data
    //
    // Authenticated Data is:
    //
    //   [messageType; byte] [messageLength; uint32] [label_data; optional]
    //
    let mut ciphertext = buf.split_off(meta_size);
    if stream_label.is_empty() {
      // Decrypt the payload
      keyring
        .decrypt_payload(&mut ciphertext, &buf)
        .map(|_| {
          buf.unsplit(ciphertext);
          buf.freeze()
        })
        .map_err(From::from)
    } else {
      let data_bytes = append_bytes(&buf, stream_label.as_bytes());
      // Decrypt the payload
      keyring
        .decrypt_payload(&mut ciphertext, data_bytes.as_ref())
        .map(|_| {
          buf.unsplit(ciphertext);
          buf.freeze()
        })
        .map_err(From::from)
    }
  }
}

struct EncryptedRemoteStateHeader {
  meta_size: usize,
  buf: BytesMut,
}
