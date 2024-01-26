use smol_str::SmolStr;
use transformable::Transformable;

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
#[cfg_attr(
  feature = "rkyv",
  archive_attr(derive(Debug, PartialEq, Eq, Hash), repr(transparent))
)]
#[repr(transparent)]
pub struct ErrorResponse {
  err: SmolStr,
}

impl ErrorResponse {
  pub fn new(err: impl Into<SmolStr>) -> Self {
    Self { err: err.into() }
  }
}

impl core::fmt::Display for ErrorResponse {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.err)
  }
}

impl std::error::Error for ErrorResponse {}

impl Transformable for ErrorResponse {
  type Error = <SmolStr as Transformable>::Error;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    self.err.encode(dst)
  }

  fn encoded_len(&self) -> usize {
    self.err.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let (len, err) = SmolStr::decode(src)?;
    Ok((len, Self { err }))
  }

  fn encode_to_vec(&self) -> Result<Vec<u8>, Self::Error> {
    <SmolStr as Transformable>::encode_to_vec(&self.err)
  }

  async fn encode_to_async_writer<W: futures::io::AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> std::io::Result<usize> {
    <SmolStr as Transformable>::encode_to_async_writer(&self.err, writer).await
  }

  fn encode_to_writer<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<usize> {
    <SmolStr as Transformable>::encode_to_writer(&self.err, writer)
  }

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    <SmolStr as Transformable>::decode_from_reader(reader).map(|(n, err)| (n, Self { err }))
  }

  async fn decode_from_async_reader<R: futures::io::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    <SmolStr as Transformable>::decode_from_async_reader(reader)
      .await
      .map(|(n, err)| (n, Self { err }))
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
