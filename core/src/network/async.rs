// use crate::{
//   error::Error,
//   label::{remove_label_header_from_stream, LabeledReader},
//   security::{append_bytes, decrypt_payload}, SecretKeyring,
// };

// use super::*;
// use bytes::{BytesMut, BufMut};
// use futures_util::{
//   future::{BoxFuture, FutureExt},
//   io::{AsyncRead, AsyncReadExt}
// };
// use showbiz_traits::Connection;
// use showbiz_types::MessageType;

// pub(crate) struct StreamProcessor<T: Transport> {
//   transport: Arc<T>,
//   shutdown_rx: async_channel::Receiver<()>,
//   conn_timeout: Duration,
//   skip_inbound_label_check: bool,
//   gossip_verify_incoming: bool,
//   label: Bytes,
//   encryption_enabled: bool,
//   keyring: SecretKeyring,
// }

// impl<T: Transport> StreamProcessor<T> {
//   pub(crate) fn new(
//     transport: Arc<T>,
//     shutdown_rx: async_channel::Receiver<()>,
//     conn_timeout: Duration,
//     skip_inbound_label_check: bool,
//     gossip_verify_incoming: bool,
//     label: Bytes,
//     encryption_enabled: bool,
//     keyring: SecretKeyring,
//   ) -> Self {
//     Self {
//       transport,
//       shutdown_rx,
//       conn_timeout,
//       skip_inbound_label_check,
//       gossip_verify_incoming,
//       label,
//       encryption_enabled,
//       keyring,
//     }
//   }

//   pub(crate) fn run<R, S>(mut self, spawner: S)
//   where
//     R: Send + Sync + 'static,
//     S: Fn(BoxFuture<'static, ()>) -> R + Copy + Send + Sync + 'static,
//   {
//     (spawner)(Box::pin(async move {
//       let Self {
//         transport,
//         shutdown_rx,
//         conn_timeout,
//         skip_inbound_label_check,
//         gossip_verify_incoming,
//         label,
//         encryption_enabled,
//         keyring,
//       } = self;
//       loop {
//         futures_util::select! {
//           _ = shutdown_rx.recv().fuse() => {
//             return;
//           }
//           conn = transport.stream().recv().fuse() => {
//             let label = label.clone();
//             let keyring = keyring.clone();
//             (spawner)(async {
//               match conn {
//                 Ok(conn) => Self::handle_conn(
//                   conn,
//                   conn_timeout,
//                   skip_inbound_label_check,
//                   gossip_verify_incoming,
//                   label,
//                   encryption_enabled,
//                   keyring
//                 ).await,
//                 Err(e) => tracing::error!(target = "showbiz", "failed to accept connection: {}", e),
//               }
//             }.boxed());
//           }
//         }
//       }
//     }));
//   }

//   async fn handle_conn(
//     mut conn: T::Connection,
//     conn_timeout: Duration,
//     skip_inbound_label_check: bool,
//     gossip_verify_incoming: bool,
//     label: Bytes,
//     encryption_enabled: bool,
//     keyring: SecretKeyring,
//   ) {
//     let addr = match <T::Connection as Connection>::remote_address(&conn) {
//       Ok(addr) => {
//         tracing::debug!(target = "showbiz", remote_addr = %addr, "stream connection");
//         Some(addr)
//       },
//       Err(e) => {
//         tracing::error!(target = "showbiz", err = %e, "fail to get connection remote address");
//         tracing::debug!(target = "showbiz", remote_addr = "unknown", "stream connection");
//         None
//       },
//     };

//     // TODO: metrics

//     if conn_timeout != Duration::ZERO {
//       <T::Connection as Connection>::set_timeout(&mut conn, Some(conn_timeout));
//     }

//     let mut lr = match remove_label_header_from_stream(conn).await {
//       Ok(lr) => lr,
//       Err(e) => {
//         tracing::error!(target = "showbiz", err = %e, remote_addr = ?addr, "failed to remove label header");
//         return;
//       },
//     };

//     if skip_inbound_label_check {
//       if lr.label().is_some() {
//         tracing::error!(target = "showbiz", remote_addr = ?addr, "unexpected double stream label header");
//         return;
//       }
//       // Set this from config so that the auth data assertions work below
//       lr.set_label(label.clone());
//     }

//     if label.ne(lr.label().unwrap()) {
//       tracing::error!(target = "showbiz", remote_addr = ?addr, "discarding stream with unacceptable label: {:?}", label.as_ref());
//       return;
//     }

//     // Read the message type
//     let mut buf = [0u8; 1];
//     let mut mt = match lr.read(&mut buf).await {
//       Ok(n) => {
//         if n == 0 {
//           tracing::error!(target = "showbiz", remote_addr = ?addr, "failed to read message type");
//           return;
//         }
//         match MessageType::try_from(buf[0]) {
//           Ok(mt) => mt,
//           Err(e) => {
//             tracing::error!(target = "showbiz", err = %e, remote_addr = ?addr, "failed to read message type");
//             return;
//           },
//         }
//       },
//       Err(e) => {
//         tracing::error!(target = "showbiz", err = %e, remote_addr = ?addr, "failed to read message type");
//         return;
//       },
//     };

//     // Check if the message is encrypted
//     if mt == MessageType::Encrypt {
//       if !encryption_enabled {
//         tracing::error!(target = "showbiz", remote_addr = ?addr, "remote state is encrypted and encryption is not configured");
//         return;
//       }

//       let plain = match decrypt_remote_state(&mut lr, label.as_ref()).await {
//         Ok(plain) => plain,
//         Err(e) => {
//           tracing::error!(target = "showbiz", err = %e, remote_addr = ?addr, "failed to decrypt remote state");
//           return;
//         },
//       };

//       // Reset message type and buf conn
//       mt = match MessageType::try_from(plain[0]) {
//         Ok(mt) => mt,
//         Err(e) => {
//           tracing::error!(target = "showbiz", err = %e, remote_addr = ?addr, "failed to read message type");
//           return;
//         },
//       };

//       // TODO: reset conn
//     } else if encryption_enabled && gossip_verify_incoming {
//       tracing::error!(target = "showbiz", remote_addr = ?addr, "encryption is configured but remote state is not encrypted");
//       return;
//     }

//     // Get the msgPack decoders

//     if mt == MessageType::Compress {

//     }

//     match mt {
//       MessageType::Ping => {}
//       MessageType::User => {}
//       MessageType::PushPull => {}
//       _ => {
//         tracing::error!(target = "showbiz", remote_addr = ?addr, "received invalid msg type {}", mt);
//       }
//     }
//   }
// }

// async fn decrypt_remote_state<R: AsyncRead + std::marker::Unpin>(r: &mut LabeledReader<R>, stream_label: &[u8]) -> Result<Bytes, Error> {
//   let mut buf = BytesMut::with_capacity(8);
//   buf.put_u8(MessageType::Encrypt as u8);
//   let mut b = [0u8; 4];
//   r.read_exact(&mut b).await?;
//   buf.put_slice(&b);

//   // Ensure we aren't asked to download too much. This is to guard against
// 	// an attack vector where a huge amount of state is sent
//   let more_bytes = u32::from_be_bytes(b) as usize;
//   if more_bytes > MAX_PUSH_STATE_BYTES {
//     return Err(Error::LargeRemoteState(more_bytes));
//   }

//   //Start reporting the size before you cross the limit
//   if more_bytes > (0.6 * (MAX_PUSH_STATE_BYTES as f64)).floor() as usize {
//     tracing::warn!(target = "showbiz", "remote state size is {} limit is large: {}", more_bytes, MAX_PUSH_STATE_BYTES);
//   }

//   // Read in the rest of the payload
//   buf.resize(5 + more_bytes as usize, 0);
//   r.read_exact(&mut buf).await?;

//   // Decrypt the cipherText with some authenticated data
// 	//
// 	// Authenticated Data is:
// 	//
// 	//   [messageType; byte] [messageLength; uint32] [label_data; optional]
// 	//
//   let data_bytes = append_bytes(&buf[..5], stream_label);
//   let cipher_text = &buf[5..];

//   // Decrypt the payload
//   // decrypt_payload(keys, cipher_text, data_bytes.as_ref())
//   todo!()
// }
