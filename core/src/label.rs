use crate::{error::Error, showbiz::Showbiz};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use showbiz_traits::{Delegate, Transport};
use showbiz_types::MessageType;

const LABEL_MAX_SIZE: usize = 255;
const DEFAULT_BUFFER_SIZE: usize = 4096;

impl<D: Delegate, T: Transport> Showbiz<T, D> {
  /// Rrefixes outgoing packets with the correct header if
  /// the label is not empty.
  pub fn add_label_header_to_packet<E>(src: &[u8], label: &[u8]) -> Result<Bytes, Error<T, D>> {
    if !label.is_empty() {
      if label.len() > LABEL_MAX_SIZE {
        return Err(Error::LabelTooLong(label.len()));
      }
      Ok(make_label_header(label, src))
    } else {
      Ok(Bytes::copy_from_slice(src))
    }
  }

  pub fn remove_label_header_from_packet(mut buf: Bytes) -> Result<(Bytes, Bytes), Error<T, D>> {
    #[allow(clippy::declare_interior_mutable_const)]
    const EMPTY_BYTES: Bytes = Bytes::new();

    if buf.is_empty() {
      return Ok((buf, EMPTY_BYTES));
    }

    let msg_type = MessageType::try_from(buf[0])?;
    if msg_type != MessageType::HasLabel {
      return Ok((buf, EMPTY_BYTES));
    }

    if buf.len() < 2 {
      return Err(Error::TruncatedLabel);
    }

    let label_size = buf[1] as usize;
    if label_size < 1 {
      return Err(Error::EmptyLabel);
    }

    if buf.len() < 2 + label_size {
      return Err(Error::TruncatedLabel);
    }

    buf.advance(2);
    let label = buf.split_to(label_size);
    Ok((buf, label))
  }
}

#[cfg(feature = "sync")]
mod sync;
#[cfg(feature = "sync")]
pub use sync::*;

#[cfg(feature = "async")]
mod r#async;
#[cfg(feature = "async")]
pub use r#async::*;

#[inline]
fn make_label_header(label: &[u8], src: &[u8]) -> Bytes {
  let mut dst = BytesMut::with_capacity(2 + src.len() + label.len());
  dst.put_u8(MessageType::HasLabel as u8);
  dst.put_u8(label.len() as u8);
  dst.put_slice(label);
  dst.put_slice(src);
  dst.freeze()
}

// #[cfg(test)]
// mod tests {
//   use showbiz_traits::VoidDelegateError;

// use super::*;
//   use crate::security::MAX_ENCRYPTION_VERSION;

//   #[derive(Debug)]
//   struct TestCase {
//     buf: Bytes,
//     expect_label: Bytes,
//     expect_packet: Bytes,
//     expect_err: Option<Error<VoidDelegateError>>,
//   }

//   fn add_stream_test_cases() -> Vec<(&'static str, TestCase)> {
//     vec![
//       (
//         "no label",
//         TestCase {
//           buf: Bytes::new(),
//           expect_label: Bytes::new(),
//           expect_packet: Bytes::new(),
//           expect_err: None,
//         },
//       ),
//       (
//         "with label",
//         TestCase {
//           buf: Bytes::new(),
//           expect_label: Bytes::from_static(b"foo"),
//           expect_packet: build_test_data(MessageType::HasLabel as u8, &[3, b'f', b'o', b'o']),
//           expect_err: None,
//         },
//       ),
//       (
//         "almost too long label",
//         TestCase {
//           buf: Bytes::new(),
//           expect_label: Bytes::from_static(&[b'a'; LABEL_MAX_SIZE]),
//           expect_packet: build_test_data(
//             MessageType::HasLabel as u8,
//             &[LABEL_MAX_SIZE as u8]
//               .into_iter()
//               .chain([b'a'; 255].into_iter())
//               .collect::<Vec<_>>(),
//           ),
//           expect_err: None,
//         },
//       ),
//       (
//         "label too long by one byte",
//         TestCase {
//           buf: Bytes::new(),
//           expect_label: [b'a'; 255]
//             .into_iter()
//             .chain([b'x'].into_iter())
//             .collect::<Vec<_>>()
//             .into(),
//           expect_packet: Bytes::new(),
//           expect_err: Some(Error::LabelTooLong(LABEL_MAX_SIZE + 1)),
//         },
//       ),
//     ]
//   }
//   fn add_test_cases() -> Vec<(&'static str, TestCase)> {
//     vec![
//       (
//         "nil buf with no label",
//         TestCase {
//           buf: Bytes::new(),
//           expect_label: Bytes::new(),
//           expect_packet: Bytes::new(),
//           expect_err: None,
//         },
//       ),
//       (
//         "nil buf with label",
//         TestCase {
//           buf: Bytes::new(),
//           expect_label: Bytes::from_static(b"foo"),
//           expect_packet: build_test_data(MessageType::HasLabel as u8, &[3, b'f', b'o', b'o']),
//           expect_err: None,
//         },
//       ),
//       (
//         "message with label",
//         TestCase {
//           buf: Bytes::from_static(b"something"),
//           expect_label: Bytes::from_static(b"foo"),
//           expect_packet: build_test_data(
//             MessageType::HasLabel as u8,
//             &[
//               3, b'f', b'o', b'o', b's', b'o', b'm', b'e', b't', b'h', b'i', b'n', b'g',
//             ],
//           ),
//           expect_err: None,
//         },
//       ),
//       (
//         "message with no label",
//         TestCase {
//           buf: Bytes::from_static(b"something"),
//           expect_label: Bytes::new(),
//           expect_packet: Bytes::from_static(b"something"),
//           expect_err: None,
//         },
//       ),
//       (
//         "message with almost too long label",
//         TestCase {
//           buf: Bytes::from_static(b"something"),
//           expect_label: Bytes::from_static(&[b'a'; LABEL_MAX_SIZE]),
//           expect_packet: build_test_data(
//             MessageType::HasLabel as u8,
//             &[LABEL_MAX_SIZE as u8]
//               .into_iter()
//               .chain([b'a'; 255].into_iter().chain((*b"something").into_iter()))
//               .collect::<Vec<_>>(),
//           ),
//           expect_err: None,
//         },
//       ),
//       (
//         "label too long by one byte",
//         TestCase {
//           buf: Bytes::from_static(b"something"),
//           expect_label: Bytes::from_static(&[0; LABEL_MAX_SIZE + 1]),
//           expect_packet: Bytes::new(),
//           expect_err: Some(Error::LabelTooLong(LABEL_MAX_SIZE + 1)),
//         },
//       ),
//     ]
//   }
//   fn remove_test_cases() -> Vec<(&'static str, TestCase)> {
//     vec![
//       (
//         "empty buf",
//         TestCase {
//           buf: Bytes::new(),
//           expect_label: Bytes::new(),
//           expect_packet: Bytes::new(),
//           expect_err: None,
//         },
//       ),
//       (
//         "ping with no label",
//         TestCase {
//           buf: build_test_data(MessageType::Ping as u8, b"blah"),
//           expect_label: Bytes::new(),
//           expect_packet: build_test_data(MessageType::Ping as u8, b"blah"),
//           expect_err: None,
//         },
//       ),
//       (
//         "error with no label",
//         TestCase {
//           buf: build_test_data(MessageType::ErrorResponse as u8, b"blah"),
//           expect_label: Bytes::new(),
//           expect_packet: build_test_data(MessageType::ErrorResponse as u8, b"blah"),
//           expect_err: None,
//         },
//       ),
//       (
//         "v1 encrypt with no label",
//         TestCase {
//           buf: build_test_data(MAX_ENCRYPTION_VERSION as u8, b"blah"),
//           expect_label: Bytes::new(),
//           expect_packet: build_test_data(MAX_ENCRYPTION_VERSION as u8, b"blah"),
//           expect_err: None,
//         },
//       ),
//       (
//         "buf too small for label",
//         TestCase {
//           buf: build_test_data(MessageType::HasLabel as u8, b"x"),
//           expect_label: Bytes::new(),
//           expect_packet: Bytes::new(),
//           expect_err: Some(Error::TruncatedLabel),
//         },
//       ),
//       (
//         "buf too small for label size",
//         TestCase {
//           buf: build_test_data(MessageType::HasLabel as u8, &[]),
//           expect_label: Bytes::new(),
//           expect_packet: Bytes::new(),
//           expect_err: Some(Error::TruncatedLabel),
//         },
//       ),
//       (
//         "label empty",
//         TestCase {
//           buf: build_test_data(MessageType::HasLabel as u8, &[0, b'x']),
//           expect_label: Bytes::new(),
//           expect_packet: Bytes::new(),
//           expect_err: Some(Error::EmptyLabel),
//         },
//       ),
//       (
//         "label truncated",
//         TestCase {
//           buf: build_test_data(MessageType::HasLabel as u8, &[2, b'x']),
//           expect_label: Bytes::new(),
//           expect_packet: Bytes::new(),
//           expect_err: Some(Error::TruncatedLabel),
//         },
//       ),
//       (
//         "ping with label",
//         TestCase {
//           buf: build_test_data(
//             MessageType::HasLabel as u8,
//             &[
//               3,
//               b'a',
//               b'b',
//               b'c',
//               MessageType::Ping as u8,
//               b'b',
//               b'l',
//               b'a',
//               b'h',
//             ],
//           ),
//           expect_label: Bytes::from_static(b"abc"),
//           expect_packet: build_test_data(MessageType::Ping as u8, b"blah"),
//           expect_err: None,
//         },
//       ),
//       (
//         "error with label",
//         TestCase {
//           buf: build_test_data(
//             MessageType::HasLabel as u8,
//             &[
//               3,
//               b'a',
//               b'b',
//               b'c',
//               MessageType::ErrorResponse as u8,
//               b'b',
//               b'l',
//               b'a',
//               b'h',
//             ],
//           ),
//           expect_label: Bytes::from_static(b"abc"),
//           expect_packet: build_test_data(MessageType::ErrorResponse as u8, b"blah"),
//           expect_err: None,
//         },
//       ),
//       (
//         "v1 encrypt with label",
//         TestCase {
//           buf: build_test_data(
//             MessageType::HasLabel as u8,
//             &[
//               3,
//               b'a',
//               b'b',
//               b'c',
//               MAX_ENCRYPTION_VERSION as u8,
//               b'b',
//               b'l',
//               b'a',
//               b'h',
//             ],
//           ),
//           expect_label: Bytes::from_static(b"abc"),
//           expect_packet: build_test_data(MAX_ENCRYPTION_VERSION as u8, b"blah"),
//           expect_err: None,
//         },
//       ),
//     ]
//   }

//   fn build_test_data(mt: u8, data: &[u8]) -> Bytes {
//     let mut buf = BytesMut::with_capacity(1 + data.len());
//     buf.put_u8(mt);
//     buf.put_slice(data);
//     buf.freeze()
//   }

//   #[test]
//   fn test_add_label_header_to_packet() {
//     fn run(tc: TestCase) {
//       let result = add_label_header_to_packet(&tc.buf, &tc.expect_label);
//       match result {
//         Ok(got_buf) => {
//           assert_eq!(tc.expect_packet, got_buf);
//         }
//         Err(e) => {
//           assert_eq!(tc.expect_err, Some(e));
//         }
//       }
//     }

//     let cases = add_test_cases();

//     for (_name, case) in cases {
//       run(case);
//     }
//   }

//   #[test]
//   fn test_remove_label_header_from_packet() {
//     fn run(tc: TestCase) {
//       let result = remove_label_header_from_packet(tc.buf);
//       match result {
//         Ok((got_buf, got_label)) => {
//           assert_eq!(tc.expect_packet, got_buf);
//           assert_eq!(tc.expect_label, got_label);
//         }
//         Err(e) => {
//           assert_eq!(tc.expect_err, Some(e));
//         }
//       }
//     }

//     let cases = remove_test_cases();

//     for (_, case) in cases {
//       run(case);
//     }
//   }

//   #[cfg(feature = "async")]
//   mod async_tests {
//     use super::*;

//     #[tokio::test]
//     async fn test_remove_label_header_from_stream() {
//       use futures_util::io::AsyncReadExt;
//       use tokio::{io::AsyncWriteExt, net::UnixStream, sync::oneshot::channel};
//       use tokio_util::compat::TokioAsyncReadCompatExt;

//       async fn run(tc: TestCase) {
//         let (mut server, client) = UnixStream::pair().unwrap();
//         let mut client = client.compat();
//         let (tx, rx) = channel::<()>();
//         tokio::spawn(async move {
//           server.write_all(&tc.buf).await.unwrap();
//           tx.send(()).unwrap();
//         });
//         rx.await.unwrap();

//         let err = remove_label_header_from_stream(&mut client).await;

//         match err {
//           Ok(mut label) => {
//             let mut got_buf = vec![0; tc.expect_packet.len()];
//             label.read_exact(&mut got_buf).await.unwrap();

//             match tc.expect_label.is_empty() {
//               true => assert_eq!(&Bytes::new(), label.label()),
//               false => assert_eq!(tc.expect_label.as_ref(), label.label()),
//             }
//             assert_eq!(tc.expect_packet, got_buf);
//           }
//           Err(err) => {
//             assert_eq!(tc.expect_err, Some(err));
//           }
//         }
//       }

//       let cases = remove_test_cases();

//       for (_name, case) in cases {
//         run(case).await;
//       }
//     }

//     #[tokio::test]
//     async fn test_add_label_header_to_stream() {
//       use futures_util::io::AsyncWriteExt;
//       use tokio::{
//         io::copy,
//         net::UnixStream,
//         sync::mpsc::{self, channel},
//       };
//       use tokio_util::compat::TokioAsyncWriteCompatExt;

//       const SUFFIX_DATA: &[u8] = b"EXTRA DATA";

//       async fn run(tc: TestCase) {
//         let (mut server, client) = UnixStream::pair().unwrap();
//         let mut client = client.compat_write();

//         let (data_tx, mut data_rx) = channel(1);
//         let (err_tx, mut err_rx) = channel(1);

//         tokio::spawn(async move {
//           let mut buffer = Vec::new();
//           if copy(&mut server, &mut buffer).await.is_err() {
//             err_tx.send(()).await.unwrap();
//           }
//           data_tx.send(buffer.to_vec()).await.unwrap();
//         });

//         let err = add_label_header_to_stream(&mut client, &tc.expect_label).await;

//         if let Err(err) = err {
//           assert_eq!(tc.expect_err, Some(err));
//           return;
//         }

//         client.write_all(SUFFIX_DATA).await.unwrap();
//         client.close().await.unwrap();

//         let mut expect = Vec::new();
//         expect.extend(tc.expect_packet);
//         expect.extend(SUFFIX_DATA);

//         match err_rx.try_recv() {
//           Ok(_) => panic!("Expected no error, but got one."),
//           Err(mpsc::error::TryRecvError::Empty) => (),
//           Err(mpsc::error::TryRecvError::Disconnected) => panic!("Error channel disconnected"),
//         }

//         let got = data_rx.recv().await.unwrap();
//         assert_eq!(expect, got);
//       }

//       let cases = add_stream_test_cases();

//       for (_, case) in cases {
//         run(case).await;
//       }
//     }
//   }

//   #[cfg(feature = "sync")]
//   mod sync_tests {
//     use super::sync::*;
//     use super::*;

//     #[test]
//     fn test_add_label_header_to_stream() {
//       use crossbeam_channel::bounded;
//       use std::{
//         io::{Read, Write},
//         os::unix::net::UnixStream,
//       };

//       const SUFFIX_DATA: &[u8] = b"EXTRA DATA";

//       fn run(tc: TestCase) {
//         let (mut server, client) = UnixStream::pair().unwrap();

//         let (data_tx, mut data_rx) = bounded(1);
//         let (err_tx, mut err_rx) = bounded(1);

//         std::thread::spawn(move || {
//           let mut buffer = Vec::new();
//           if copy(&mut server, &mut buffer).is_err() {
//             err_tx.send(()).unwrap();
//           }
//           data_tx.send(buffer.to_vec()).unwrap();
//         });

//         let err = add_label_header_to_stream(&mut client, &tc.expect_label);

//         if let Err(err) = err {
//           assert_eq!(tc.expect_err, Some(err));
//           return;
//         }

//         client.write_all(SUFFIX_DATA).unwrap();
//         client.close().unwrap();

//         let mut expect = Vec::new();
//         expect.extend(tc.expect_packet);
//         expect.extend(SUFFIX_DATA);

//         match err_rx.try_recv() {
//           Ok(_) => panic!("Expected no error, but got one."),
//           Err(mpsc::error::TryRecvError::Empty) => (),
//           Err(mpsc::error::TryRecvError::Disconnected) => panic!("Error channel disconnected"),
//         }

//         let got = data_rx.recv().unwrap();
//         assert_eq!(expect, got);
//       }

//       let cases = add_stream_test_cases();

//       for (_, case) in cases {
//         run(case);
//       }
//     }

//     #[test]
//     fn test_remove_label_header_from_stream() {
//       use crossbeam_channel::bounded;
//       use std::{
//         io::{Read, Write},
//         os::unix::net::UnixStream,
//       };

//       fn run(tc: TestCase) {
//         let (mut server, mut client) = UnixStream::pair().unwrap();
//         let (tx, rx) = bounded::<()>(1);
//         std::thread::spawn(move || {
//           server.write_all(&tc.buf).unwrap();
//           tx.send(()).unwrap();
//         });
//         rx.recv().unwrap();

//         let err = remove_label_header_from_stream(&mut client);

//         match err {
//           Ok(mut label) => {
//             let mut got_buf = vec![0; tc.expect_packet.len()];
//             label.read_exact(&mut got_buf).unwrap();

//             match tc.expect_label.is_empty() {
//               true => assert_eq!(None, label.label()),
//               false => assert_eq!(Some(tc.expect_label).as_ref(), label.label()),
//             }
//             assert_eq!(tc.expect_packet, got_buf);
//           }
//           Err(err) => {
//             assert_eq!(tc.expect_err, Some(err));
//           }
//         }
//       }

//       let cases = remove_test_cases();

//       for (_name, case) in cases {
//         run(case);
//       }
//     }
//   }
// }
