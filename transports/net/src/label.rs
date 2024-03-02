use std::io;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, Future};
use peekable::future::AsyncPeekable;

use super::Label;

pub(crate) trait LabelAsyncIOExt: Unpin + Send + Sync {
  fn add_label_header(&mut self, label: &Label) -> impl Future<Output = io::Result<()>> + Send
  where
    Self: AsyncWrite + Unpin,
  {
    async move {
      let mut buf = BytesMut::with_capacity(2 + label.len());
      buf.put_u8(Label::TAG);
      buf.put_u8(label.len() as u8);
      buf.put_slice(label.as_bytes());

      self.write_all(&buf).await
    }
  }
}

pub(super) async fn remove_label_header<R: agnostic::Runtime>(
  this: &mut super::Deadline<AsyncPeekable<impl AsyncRead + Send + Unpin>>,
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

impl<T: AsyncWrite + AsyncRead + Unpin + Send + Sync + 'static> LabelAsyncIOExt for T {}
