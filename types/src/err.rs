use smol_str::SmolStr;
use transformable::Transformable;

/// Error response from the remote peer
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(
  feature = "rkyv",
  rkyv(derive(Debug, PartialEq, Eq, Hash), compare(PartialEq))
)]
#[repr(transparent)]
pub struct ErrorResponse {
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the msg of the error response")
    ),
    setter(attrs(doc = "Sets the msg of the error response (Builder pattern)"))
  )]
  message: SmolStr,
}

impl ErrorResponse {
  /// Create a new error response
  pub fn new(message: impl Into<SmolStr>) -> Self {
    Self {
      message: message.into(),
    }
  }

  /// Returns the msg of the error response
  pub fn set_message(&mut self, msg: impl Into<SmolStr>) -> &mut Self {
    self.message = msg.into();
    self
  }
}

impl core::fmt::Display for ErrorResponse {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.message)
  }
}

impl std::error::Error for ErrorResponse {}

impl From<ErrorResponse> for SmolStr {
  fn from(err: ErrorResponse) -> Self {
    err.message
  }
}

impl From<SmolStr> for ErrorResponse {
  fn from(msg: SmolStr) -> Self {
    Self { message: msg }
  }
}

impl Transformable for ErrorResponse {
  type Error = <SmolStr as Transformable>::Error;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    self.message.encode(dst)
  }

  fn encoded_len(&self) -> usize {
    self.message.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let (len, message) = SmolStr::decode(src)?;
    Ok((len, Self { message }))
  }

  async fn encode_to_async_writer<W: futures::io::AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> std::io::Result<usize> {
    <SmolStr as Transformable>::encode_to_async_writer(&self.message, writer).await
  }

  fn encode_to_writer<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<usize> {
    <SmolStr as Transformable>::encode_to_writer(&self.message, writer)
  }

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    <SmolStr as Transformable>::decode_from_reader(reader).map(|(n, message)| (n, Self { message }))
  }

  async fn decode_from_async_reader<R: futures::io::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    <SmolStr as Transformable>::decode_from_async_reader(reader)
      .await
      .map(|(n, message)| (n, Self { message }))
  }
}

#[cfg(test)]
const _: () = {
  use rand::{distributions::Alphanumeric, Rng};

  impl ErrorResponse {
    fn generate(size: usize) -> Self {
      let rng = rand::thread_rng();
      let err = rng
        .sample_iter(&Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      let err = String::from_utf8(err).unwrap();
      Self::new(err)
    }
  }
};

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_error_response() {
    for i in 0..100 {
      let err = ErrorResponse::generate(i);
      let mut buf = vec![0; err.encoded_len()];
      let encoded_len = err.encode(&mut buf).unwrap();
      assert_eq!(encoded_len, err.encoded_len());
      let (decoded_len, decoded) = ErrorResponse::decode(&buf).unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, err);
    }
  }
}
