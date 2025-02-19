use crate::{EncodeError, Message};

use super::{Data, Payload, ProtoEncoder, ProtoEncoderError};

impl<I, A, B> ProtoEncoder<I, A, B>
where
  I: Data + 'static,
  A: Data + 'static,
  B: AsRef<[Message<I, A>]> + Send + Sync + 'static,
{
  /// Encodes the messages.
  #[auto_enums::auto_enum(Iterator, Debug)]
  pub async fn blocking_encode<RT>(
    self,
  ) -> impl Iterator<Item = Result<Payload, ProtoEncoderError>> + 'static
  where
    RT: agnostic_lite::RuntimeLite,
  {
    let res = RT::spawn_blocking(move || self.encode().collect::<Vec<_>>()).await;

    match res {
      Ok(res) => res.into_iter(),
      Err(err) => core::iter::once(Err(EncodeError::custom(err.to_string()).into())),
    }
  }
}
