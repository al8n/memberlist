use super::{Batch, Data, Encodable, EncodeBuffer, ProtoEncoder, ProtoEncoderError, SmallVec};

use rayon::iter::{self, IntoParallelIterator, ParallelIterator};

impl<I, A> ProtoEncoder<'_, I, A>
where
  I: Data,
  A: Data,
{
  /// Encodes the messages.
  #[auto_enums::auto_enum(rayon::ParallelIterator, Debug)]
  pub fn encode_parallel(
    &self,
  ) -> impl ParallelIterator<Item = Result<Vec<u8>, ProtoEncoderError>> + core::fmt::Debug + '_ {
    match self.msgs.len() {
      0 => iter::empty(),
      1 => {
        let msg = &self.msgs[0];
        let encoded_len = msg.encoded_len();
        if let Err(err) = self.valid() {
          return rayon::iter::once(Err(err));
        }

        match self.hint_with_size(encoded_len) {
          Ok(hint) => iter::once(self.encode_single(msg, hint)),
          Err(err) => iter::once(Err(err)),
        }
      }
      _ => {
        if let Err(err) = self.valid() {
          return iter::once(Err(err));
        }

        self.encode_batch_parallel()
      }
    }
  }

  #[auto_enums::auto_enum(rayon::ParallelIterator, Debug)]
  fn into_par_iter(
    batches: SmallVec<Batch<'_, I, A>>,
  ) -> impl ParallelIterator<Item = Batch<'_, I, A>> + core::fmt::Debug + '_ {
    match batches.into_either() {
      either::Either::Left(batch) => batch.into_par_iter(),
      either::Either::Right(batches) => batches.into_vec().into_par_iter(),
    }
  }

  fn encode_batch_parallel(
    &self,
  ) -> impl ParallelIterator<Item = Result<Vec<u8>, ProtoEncoderError>> + core::fmt::Debug + '_ {
    Self::into_par_iter(self.batch().collect::<SmallVec<_>>()).map(|batch| match batch {
      Batch::One { msg, hint } => self.encode_single(msg, hint),
      Batch::More {
        msgs,
        hint,
        num_msgs,
      } => {
        let mut buf = EncodeBuffer::with_capacity(hint.input_size);
        buf.resize(hint.input_size, 0);
        buf[0] = crate::message::COMPOOUND_MESSAGE_TAG;
        buf[1] = num_msgs as u8;
        let res =
          match msgs
            .iter()
            .take(num_msgs)
            .try_fold((2, &mut buf), |(mut offset, buf), msg| {
              match msg.encodable_encode(&mut buf[offset..]) {
                Ok(written) => {
                  offset += written;
                  Ok((offset, buf))
                }
                Err(err) => Err(err),
              }
            }) {
            Ok((final_size, buf)) => {
              #[cfg(debug_assertions)]
              assert_eq!(
                final_size, hint.input_size,
                "the actual encoded length {} does not match the encoded length {} in hint",
                final_size, hint.input_size
              );

              self.encode_helper(&buf, hint)
            }
            Err(err) => Err(err.into()),
          };

        res
      }
    })
  }
}
