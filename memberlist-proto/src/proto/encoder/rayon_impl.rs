use super::{
  super::{Message, BATCH_OVERHEAD, COMPOOUND_MESSAGE_TAG},
  Batch, Data, Encodable, EncodeBuffer, Payload, ProtoEncoder, ProtoEncoderError,
};

use smallvec_wrapper::SmallVec;

use rayon::iter::{self, IntoParallelIterator, ParallelIterator};

impl<I, A, B> ProtoEncoder<I, A, B>
where
  I: Data,
  A: Data,
  B: AsRef<[Message<I, A>]> + Send + Sync,
{
  /// Encodes the messages.
  #[auto_enums::auto_enum(rayon::ParallelIterator, Debug)]
  pub fn rayon_encode(
    &self,
  ) -> impl ParallelIterator<Item = Result<Payload, ProtoEncoderError>> + core::fmt::Debug + '_ {
    let msgs = self.msgs.as_ref();
    match msgs.len() {
      0 => iter::empty(),
      1 => {
        let msg = &msgs[0];
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
  ) -> impl ParallelIterator<Item = Result<Payload, ProtoEncoderError>> + core::fmt::Debug + '_ {
    Self::into_par_iter(self.batch().collect::<SmallVec<_>>()).map(|batch| match batch {
      Batch::One { msg, hint } => self.encode_single(msg, hint),
      Batch::More {
        msgs,
        hint,
        num_msgs,
      } => {
        let mut buf = EncodeBuffer::with_capacity(hint.input_size);
        buf.resize(hint.input_size, 0);
        buf[0] = COMPOOUND_MESSAGE_TAG;
        buf[1] = num_msgs as u8;
        let res = match msgs.iter().take(num_msgs).try_fold(
          (BATCH_OVERHEAD, &mut buf),
          |(mut offset, buf), msg| match msg.encodable_encode(&mut buf[offset..]) {
            Ok(written) => {
              offset += written;
              Ok((offset, buf))
            }
            Err(err) => Err(err),
          },
        ) {
          Ok((final_size, buf)) => {
            #[cfg(debug_assertions)]
            assert_eq!(
              final_size, hint.input_size,
              "the actual encoded length {} does not match the encoded length {} in hint",
              final_size, hint.input_size
            );
            buf[2..BATCH_OVERHEAD]
              .copy_from_slice(&((final_size - BATCH_OVERHEAD) as u32).to_be_bytes());
            self.encode_helper(&buf, hint)
          }
          Err(err) => Err(err.into()),
        };

        res
      }
    })
  }
}
