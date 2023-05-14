use super::*;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};

pub struct LabeledReader<R> {
  label: Option<Bytes>,
  reader: BufReader<R>,
}

impl<R> LabeledReader<R> {
  /// Returns the label if present.
  #[inline]
  pub fn label(&self) -> Option<&Bytes> {
    self.label.as_ref()
  }

  #[inline]
  pub fn set_label(&mut self, label: Bytes) {
    self.label = Some(label)
  }
}

impl<R: Read> LabeledReader<R> {
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

impl<R: Read> Read for LabeledReader<R> {
  fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    self.reader.read(buf)
  }
}

impl<R: Read> BufRead for LabeledReader<R> {
  fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
    self.reader.fill_buf()
  }

  fn consume(&mut self, amt: usize) {
    self.reader.consume(amt)
  }
}

pub fn add_label_header_to_stream<W: Write>(w: &mut W, label: &[u8]) -> Result<(), Error> {
  if label.is_empty() {
    return Ok(());
  }

  if label.len() > LABEL_MAX_SIZE {
    return Err(Error::LabelTooLong(label.len()));
  }

  let mut w = BufWriter::with_capacity(2 + label.len(), w);
  let w = w.get_mut();
  w.write_all(&[MessageType::HasLabel as u8, label.len() as u8])
    .and_then(|_| w.write_all(label))
    .map_err(From::from)
}

/// Removes any label header from the beginning of
/// the stream if present and returns it.
pub fn remove_label_header_from_stream<R: Read>(reader: R) -> Result<LabeledReader<R>, Error> {
  let mut r = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, reader);
  let buf = match r.fill_buf() {
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
  r.consume(2 + label_size);
  Ok(LabeledReader::with_label(r, label))
}
