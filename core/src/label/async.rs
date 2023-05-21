use crate::showbiz::Showbiz;

use super::*;
use futures_util::{
  io::{AsyncBufRead, AsyncRead, BufReader},
  AsyncBufReadExt, AsyncWriteExt,
};
use showbiz_traits::{Broadcast, Delegate};

pin_project_lite::pin_project! {
  #[derive(Debug)]
  pub struct LabeledConnection<R> {
    label: Bytes,
    #[pin]
    pub(crate) conn: BufReader<R>,
  }
}

impl<R> LabeledConnection<R> {
  /// Returns the label if present.
  #[inline]
  pub const fn label(&self) -> &Bytes {
    &self.label
  }

  #[inline]
  pub fn set_label(&mut self, label: Bytes) {
    self.label = label;
  }
}

impl<R: AsyncRead> LabeledConnection<R> {
  #[inline]
  pub(crate) fn new(reader: BufReader<R>) -> Self {
    Self {
      label: Bytes::new(),
      conn: reader,
    }
  }

  #[inline]
  pub(crate) fn with_label(reader: BufReader<R>, label: Bytes) -> Self {
    Self {
      label,
      conn: reader,
    }
  }
}

impl<R: AsyncRead> AsyncRead for LabeledConnection<R> {
  fn poll_read(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
    buf: &mut [u8],
  ) -> std::task::Poll<futures_io::Result<usize>> {
    self.project().conn.poll_read(cx, buf)
  }
}

impl<R: AsyncBufRead> AsyncBufRead for LabeledConnection<R> {
  fn poll_fill_buf(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<futures_io::Result<&[u8]>> {
    self.project().conn.poll_fill_buf(cx)
  }

  fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
    self.project().conn.consume(amt)
  }
}

impl<B: Broadcast, D: Delegate, T: Transport> Showbiz<B, T, D> {
  pub async fn add_label_header_to_stream<W: futures_io::AsyncWrite + std::marker::Unpin>(
    w: &mut W,
    label: &[u8],
  ) -> Result<(), Error<B, T, D>> {
    if label.is_empty() {
      return Ok(());
    }

    if label.len() > LABEL_MAX_SIZE {
      return Err(Error::LabelTooLong(label.len()));
    }

    w.write_all(&[MessageType::HasLabel as u8, label.len() as u8])
      .await?;
    w.write_all(label).await.map_err(From::from)
  }

  /// Removes any label header from the beginning of
  /// the stream if present and returns it.
  pub async fn remove_label_header_from_stream<R: futures_io::AsyncRead + std::marker::Unpin>(
    reader: R,
  ) -> Result<LabeledConnection<R>, Error<B, T, D>> {
    let mut r = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, reader);
    let buf = match r.fill_buf().await {
      Ok(buf) => {
        if buf.is_empty() {
          return Ok(LabeledConnection::new(r));
        }
        buf
      }
      Err(e) => {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
          return Ok(LabeledConnection::new(r));
        } else {
          return Err(e.into());
        }
      }
    };

    // First check for the type byte.
    if MessageType::try_from(buf[0])? != MessageType::HasLabel {
      return Ok(LabeledConnection::new(r));
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

    let label = Bytes::copy_from_slice(&buf[2..2 + label_size]);
    r.consume_unpin(2 + label_size);
    Ok(LabeledConnection::with_label(r, label))
  }
}
