use std::io;

use bytes::Bytes;
use futures::AsyncRead;
use peekable::future::AsyncPeekable;

use super::Label;

pub(super) async fn remove_label_header<R: agnostic::Runtime>(
  this: &mut super::Deadline<AsyncPeekable<impl AsyncRead + Send + Unpin>, R::Instant>,
) -> io::Result<Option<Label>> {
  let mut meta = [0u8; 2];
  this.peek_exact::<R>(&mut meta).await?;
  if meta[0] != Label::TAG {
    return Ok(None);
  }

  let len = meta[1] as usize;
  let mut buf = vec![0u8; len];
  this.read_exact::<R>(&mut meta).await?;
  this.read_exact::<R>(&mut buf).await?;
  let buf: Bytes = buf.into();
  Label::try_from(buf)
    .map(Some)
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
