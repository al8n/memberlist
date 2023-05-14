use super::*;
use futures_util::{
  io::{AsyncBufRead, AsyncRead, BufReader},
  AsyncBufReadExt, AsyncWriteExt,
};

pin_project_lite::pin_project! {
  #[derive(Debug)]
  pub struct LabeledReader<R> {
    label: Option<Bytes>,
    #[pin]
    pub(super) reader: BufReader<R>,
  }
}

impl<R> LabeledReader<R> {
  /// Returns the label if present.
  #[inline]
  pub const fn label(&self) -> Option<&Bytes> {
    self.label.as_ref()
  }

  #[inline]
  pub fn set_label(&mut self, label: Bytes) {
    self.label = Some(label)
  }
}

impl<R: AsyncRead> LabeledReader<R> {
  #[inline]
  pub(crate) fn new(reader: BufReader<R>) -> Self {
    Self {
      label: None,
      reader,
    }
  }

  #[inline]
  pub(crate) fn with_label(reader: BufReader<R>, label: Bytes) -> Self {
    Self {
      label: Some(label),
      reader,
    }
  }
}

impl<R: AsyncRead> AsyncRead for LabeledReader<R> {
  fn poll_read(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
    buf: &mut [u8],
  ) -> std::task::Poll<futures_io::Result<usize>> {
    self.project().reader.poll_read(cx, buf)
  }
}

impl<R: AsyncBufRead> AsyncBufRead for LabeledReader<R> {
  fn poll_fill_buf(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<futures_io::Result<&[u8]>> {
    self.project().reader.poll_fill_buf(cx)
  }

  fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
    self.project().reader.consume(amt)
  }
}

pub async fn add_label_header_to_stream<W: futures_io::AsyncWrite + std::marker::Unpin>(
  w: &mut W,
  label: &[u8],
) -> Result<(), Error> {
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
) -> Result<LabeledReader<R>, Error> {
  let mut r = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, reader);
  let buf = match r.fill_buf().await {
    Ok(buf) => {
      if buf.is_empty() {
        return Ok(LabeledReader::new(r));
      }
      buf
    }
    Err(e) => {
      if e.kind() == std::io::ErrorKind::UnexpectedEof {
        return Ok(LabeledReader::new(r));
      } else {
        return Err(e.into());
      }
    }
  };

  // First check for the type byte.
  if MessageType::try_from(buf[0])? != MessageType::HasLabel {
    return Ok(LabeledReader::new(r));
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
  Ok(LabeledReader::with_label(r, label))
}
