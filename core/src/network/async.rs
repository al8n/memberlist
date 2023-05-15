use crate::{
  error::Error,
  label::{remove_label_header_from_stream, LabeledConnection},
  security::{append_bytes, encrypted_length, EncryptionAlgo, SecurityError},
  showbiz::ShowbizDelegates,
  util::{compress_payload, decompress_buffer, CompressError},
  Options, SecretKeyring,
};

use super::*;
use bytes::{BufMut, BytesMut};
use futures_util::{
  future::{BoxFuture, FutureExt},
  io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
};
use showbiz_traits::{
  AliveDelegate, ConflictDelegate, Connection, Delegate, EventDelegate, MergeDelegate,
  PingDelegate, VoidAliveDelegate, VoidConflictDelegate, VoidDelegate, VoidEventDelegate,
  VoidMergeDelegate, VoidPingDelegate,
};
use showbiz_types::{InvalidMessageType, MessageType};

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
  Decode(#[from] rmp_serde::decode::Error),
  #[error("{0}")]
  Compress(#[from] CompressError),
  #[error("{0}")]
  Security(#[from] SecurityError),
}

pub(crate) struct StreamProcessor<
  T: Transport,
  D = VoidDelegate,
  ED = VoidEventDelegate,
  CD = VoidConflictDelegate,
  MD = VoidMergeDelegate<Error>,
  PD = VoidPingDelegate,
  AD = VoidAliveDelegate<Error>,
> {
  transport: Arc<T>,
  shutdown_rx: async_channel::Receiver<()>,
  keyring: Option<SecretKeyring>,
  opts: Arc<Options>,
  delegates: Arc<ShowbizDelegates<D, ED, CD, MD, PD, AD>>,
}

impl<T, D, ED, CD, MD, PD, AD> StreamProcessor<T, D, ED, CD, MD, PD, AD>
where
  T: Transport,
  D: Delegate,
  ED: EventDelegate,
  CD: ConflictDelegate,
  MD: MergeDelegate,
  PD: PingDelegate,
  AD: AliveDelegate,
{
  pub(crate) fn new(
    transport: Arc<T>,
    shutdown_rx: async_channel::Receiver<()>,
    keyring: Option<SecretKeyring>,
    opts: Arc<Options>,
    delegates: Arc<ShowbizDelegates<D, ED, CD, MD, PD, AD>>,
  ) -> Self {
    Self {
      transport,
      shutdown_rx,
      keyring,
      opts,
      delegates,
    }
  }

  pub(crate) fn run<R, S>(mut self, spawner: S)
  where
    R: Send + Sync + 'static,
    S: Fn(BoxFuture<'static, ()>) -> R + Copy + Send + Sync + 'static,
  {
    (spawner)(Box::pin(async move {
      let Self {
        transport,
        shutdown_rx,
        opts,
        keyring,
        delegates,
      } = self;
      loop {
        futures_util::select! {
          _ = shutdown_rx.recv().fuse() => {
            return;
          }
          conn = transport.stream().recv().fuse() => {
            let keyring = keyring.clone();
            let opts = opts.clone();
            let delegates = delegates.clone();
            (spawner)(async {
              match conn {
                Ok(conn) => Self::handle_conn(
                  conn,
                  opts,
                  delegates,
                  keyring,
                ).await,
                Err(e) => tracing::error!(target = "showbiz", "failed to accept connection: {}", e),
              }
            }.boxed());
          }
        }
      }
    }));
  }

  async fn handle_conn(
    mut conn: T::Connection,
    opts: Arc<Options>,
    delegates: Arc<ShowbizDelegates<D, ED, CD, MD, PD, AD>>,
    keyring: Option<SecretKeyring>,
  ) {
    let addr = match <T::Connection as Connection>::remote_address(&conn) {
      Ok(addr) => {
        tracing::debug!(target = "showbiz", remote_addr = %addr, "stream connection");
        Some(addr)
      }
      Err(e) => {
        tracing::error!(target = "showbiz", err = %e, "fail to get connection remote address");
        tracing::debug!(
          target = "showbiz",
          remote_addr = "unknown",
          "stream connection"
        );
        None
      }
    };

    // TODO: metrics

    if opts.tcp_timeout != Duration::ZERO {
      <T::Connection as Connection>::set_timeout(&mut conn, Some(opts.tcp_timeout));
    }

    let mut lr = match remove_label_header_from_stream(conn).await {
      Ok(lr) => lr,
      Err(e) => {
        tracing::error!(target = "showbiz", err = %e, remote_addr = ?addr, "failed to remove label header");
        return;
      }
    };

    if opts.skip_inbound_label_check {
      if !lr.label().is_empty() {
        tracing::error!(target = "showbiz", remote_addr = ?addr, "unexpected double stream label header");
        return;
      }
      // Set this from config so that the auth data assertions work below
      lr.set_label(opts.label.clone());
    }

    if opts.label.ne(lr.label()) {
      tracing::error!(target = "showbiz", remote_addr = ?addr, "discarding stream with unacceptable label: {:?}", opts.label.as_ref());
      return;
    }

    let encryption_enabled = if let Some(keyring) = &keyring {
      keyring.lock().await.is_empty()
    } else {
      false
    };

    let (lr, mt) = match Self::read_stream(lr, encryption_enabled, keyring.as_ref(), &opts).await {
      Ok((mt, lr)) => (mt, lr),
      Err(e) => match e {
        InnerError::IO(e) => {
          if e.kind() != std::io::ErrorKind::UnexpectedEof {
            tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to receive");
          }
          if let Err(e) = Self::encode_and_send_err_msg(
            lr,
            ErrorResponse {
              err: e.to_string().into(),
            },
            addr.as_ref(),
            keyring.as_ref(),
            opts.compression_algo,
            opts.encryption_algo,
            encryption_enabled,
            opts.gossip_verify_outgoing,
          )
          .await
          {
            tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to send error response");
            return;
          }
          return;
        }
        e => {
          tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to receive");
          return;
        }
      },
    };

    match mt {
      MessageType::Ping => {}
      MessageType::User => {}
      MessageType::PushPull => {}
      mt => {
        tracing::error!(target = "showbiz", remote_addr = ?addr, "received invalid msg type {}", mt);
      }
    }
  }

  async fn encode_and_send_err_msg(
    mut lr: LabeledConnection<T::Connection>,
    err: ErrorResponse,
    addr: Option<&SocketAddr>,
    keyring: Option<&SecretKeyring>,
    compression_algo: CompressionAlgo,
    encryption_algo: EncryptionAlgo,
    encryption_enabled: bool,
    gossip_verify_outgoing: bool,
  ) -> Result<(), InnerError> {
    let mut buf = Vec::with_capacity(8);
    if let Err(e) = rmp::encode::write_u8(&mut buf, MessageType::Err as u8)
      .and_then(|_| rmp::encode::write_str(&mut buf, err.err.as_str()))
    {
      tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to encode error response");
      return Err(e.into());
    }

    // Check if compression is enabled
    if !compression_algo.is_none() {
      buf = match compress_payload(compression_algo, &buf) {
        Ok(buf) => buf,
        Err(e) => {
          tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to compress payload");
          return Err(e.into());
        }
      }
    }

    // Check if encryption is enabled
    if encryption_enabled && gossip_verify_outgoing {
      match encrypt_local_state(keyring.unwrap(), &buf, lr.label(), encryption_algo).await {
        // Write out the entire send buffer
        Ok(crypt) => lr.conn.write_all(&buf).await.map_err(From::from),
        Err(e) => {
          tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to encrypt local state");
          return Err(e.into());
        }
      }
    } else {
      // Write out the entire send buffer
      //TODO: handle metrics
      lr.conn.write_all(&buf).await.map_err(From::from)
    }
  }

  async fn read_stream<R: AsyncRead + Unpin>(
    mut lr: LabeledConnection<R>,
    encryption_enabled: bool,
    keyring: Option<&SecretKeyring>,
    opts: &Options,
  ) -> Result<(LabeledConnection<R>, MessageType), InnerError> {
    // Read the message type
    let mut buf = [0u8; 1];
    let mut mt = match lr.read(&mut buf).await {
      Ok(n) => {
        if n == 0 {
          return Err(InnerError::IO(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof",
          )));
        }
        match MessageType::try_from(buf[0]) {
          Ok(mt) => mt,
          Err(e) => {
            return Err(InnerError::from(e));
          }
        }
      }
      Err(e) => {
        return Err(InnerError::IO(e));
      }
    };

    // Check if the message is encrypted
    if mt == MessageType::Encrypt {
      if !encryption_enabled {
        return Err(InnerError::Other(
          "remote state is encrypted and encryption is not configured",
        ));
      }

      let Some(keyring) = keyring else {
        return Err(InnerError::Other(
          "remote state is encrypted and encryption is not configured",
        ));
      };

      let plain = match decrypt_remote_state(&mut lr, keyring).await {
        Ok(plain) => plain,
        Err(e) => {
          // TODO: handle e
          return Err(InnerError::Other("failed to decrypt remote state"));
        }
      };

      // Reset message type and buf conn
      mt = match MessageType::try_from(plain[0]) {
        Ok(mt) => mt,
        Err(e) => {
          return Err(InnerError::from(e));
        }
      };

      // TODO: reset conn
    } else if encryption_enabled && opts.gossip_verify_incoming {
      return Err(InnerError::Other(
        "encryption is configured but remote state is not encrypted",
      ));
    }

    if mt == MessageType::Compress {
      let mut compressed_size = [0u8; core::mem::size_of::<u32>()];
      lr.read_exact(&mut compressed_size).await?;
      let compressed_size = u32::from_be_bytes(compressed_size) as usize;
      let mut buf = vec![0; compressed_size];
      lr.read_exact(&mut buf).await?;
      let compress = rmp_serde::from_slice::<'_, Compress>(&buf)?;
      let uncompressed_data = if compress.algo.is_none() {
        compress.buf
      } else {
        decompress_buffer(compress.algo, &compress.buf).map(Into::into)?
      };

      // Reset the message type
      mt = match MessageType::try_from(uncompressed_data[0]) {
        Ok(mt) => mt,
        Err(e) => {
          return Err(InnerError::from(e));
        }
      };

      // Create a new bufConn
    }

    todo!()
  }
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
