use memberlist_utils::{SmallVec, TinyVec};
use nodecraft::Transformable;

use crate::types::Message;

pub(crate) fn retransmit_limit(retransmit_mult: usize, n: usize) -> usize {
  let node_scale = ((n + 1) as f64).log10().ceil() as usize;
  retransmit_mult * node_scale
}

/// Returns the hostname of the current machine.
///
/// On wasm target, this function always returns `None`.
///
/// # Examples
///
/// ```
/// use memberlist_core::util::hostname;
///
/// let hostname = hostname();
/// println!("hostname: {hostname:?}");
/// ```
#[allow(unreachable_code)]
pub fn hostname() -> Option<String> {
  #[cfg(not(any(target_arch = "wasm32", windows)))]
  return {
    let name = rustix::system::uname();
    let name = name.nodename();
    name
      .is_empty()
      .then_some(name.to_string_lossy().to_string())
  };

  #[cfg(windows)]
  return {
    match ::hostname::get() {
      Ok(name) => {
        let name = name.to_string_lossy();
        name.is_empty().then_some(name.to_string())
      }
      Err(_) => None,
    }
  };

  None
}

/// A batch of messages.
#[derive(Debug, Clone)]
pub enum Batch<I, A> {
  /// Batch contains only one [`Message`].
  One {
    /// The message in this batch.
    msg: Message<I, A>,
    /// The estimated encoded size of this [`Message`].
    estimate_encoded_size: usize,
  },
  /// Batch contains multiple [`Message`]s.
  More {
    /// The estimated encoded size of this batch.
    estimate_encoded_size: usize,
    /// The messages in this batch.
    msgs: TinyVec<Message<I, A>>,
  },
}

impl<I, A> IntoIterator for Batch<I, A> {
  type Item = Message<I, A>;

  type IntoIter = <TinyVec<Message<I, A>> as IntoIterator>::IntoIter;

  fn into_iter(self) -> Self::IntoIter {
    match self {
      Self::One { msg, .. } => TinyVec::from(msg).into_iter(),
      Self::More { msgs, .. } => msgs.into_iter(),
    }
  }
}

impl<I, A> Batch<I, A> {
  /// Returns the estimated encoded size for this batch.
  #[inline]
  pub const fn estimate_encoded_size(&self) -> usize {
    match self {
      Self::One {
        estimate_encoded_size,
        ..
      } => *estimate_encoded_size,
      Self::More {
        estimate_encoded_size,
        ..
      } => *estimate_encoded_size,
    }
  }

  /// Returns the number of messages in this batch.
  #[inline]
  pub fn len(&self) -> usize {
    match self {
      Self::One { .. } => 1,
      Self::More { msgs, .. } => msgs.len(),
    }
  }

  /// Returns `true` if this batch is empty.
  #[inline]
  pub fn is_empty(&self) -> bool {
    match self {
      Self::One { .. } => false,
      Self::More { msgs, .. } => msgs.is_empty(),
    }
  }
}

/// Used to indicate how to batch a collection of messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchHint {
  /// Batch should contains only one [`Message`]
  One {
    /// The index of this message belongs to the original slice
    idx: usize,
    /// The encoded size of this message
    encoded_size: usize,
  },
  /// Batch should contains multiple  [`Message`]s
  More {
    /// The range of this batch belongs to the original slice
    range: core::ops::Range<usize>,
    /// The encoded size of this batch
    encoded_size: usize,
  },
}

/// Calculate batch hints for a slice of messages.
fn batch_hints<I, A, W>(
  fixed_payload_overhead: usize,
  batch_overhead: usize,
  msg_overhead: usize,
  max_encoded_batch_size: usize,
  max_encoded_message_size: usize,
  max_messages_per_batch: usize,
  msgs: &[Message<I, A>],
) -> SmallVec<BatchHint>
where
  I: Transformable,
  A: Transformable,
  W: crate::transport::Wire<Id = I, Address = A>,
{
  let mut infos = SmallVec::new();
  let mut current_encoded_size = fixed_payload_overhead + batch_overhead;
  let mut batch_start_idx = 0;
  let total_len = msgs.len();

  if total_len == 0 {
    return infos;
  }

  if total_len == 1 {
    let msg_encoded_len = W::encoded_len(&msgs[0]);
    infos.push(BatchHint::One {
      idx: 0,
      encoded_size: fixed_payload_overhead + msg_encoded_len,
    });
    return infos;
  }

  for (idx, msg) in msgs.iter().enumerate() {
    let msg_encoded_len = W::encoded_len(msg);
    if msg_encoded_len > max_encoded_message_size {
      infos.push(BatchHint::One {
        idx,
        encoded_size: fixed_payload_overhead + msg_encoded_len,
      });
      continue;
    }

    let need = msg_overhead + msg_encoded_len;
    if idx + 1 == total_len {
      infos.push(BatchHint::More {
        range: batch_start_idx..idx,
        encoded_size: current_encoded_size + need,
      });
      return infos;
    }

    if need + current_encoded_size >= max_encoded_batch_size
      || idx > max_messages_per_batch + batch_start_idx
    {
      infos.push(BatchHint::More {
        range: batch_start_idx..idx,
        encoded_size: current_encoded_size,
      });
      current_encoded_size = fixed_payload_overhead + batch_overhead + need;
      batch_start_idx = idx;
      continue;
    }

    current_encoded_size += need;
  }

  infos
}

/// Batch a collection of messages.
pub fn batch<I, A, M, W>(
  fixed_payload_overhead: usize,
  batch_overhead: usize,
  msg_overhead: usize,
  max_encoded_batch_size: usize,
  max_encoded_message_size: usize,
  max_messages_per_batch: usize,
  msgs: M,
) -> SmallVec<Batch<I, A>>
where
  I: Transformable,
  A: Transformable,
  M: AsRef<[Message<I, A>]> + IntoIterator<Item = Message<I, A>>,
  W: crate::transport::Wire<Id = I, Address = A>,
{
  let hints = batch_hints::<_, _, W>(
    fixed_payload_overhead,
    batch_overhead,
    msg_overhead,
    max_encoded_batch_size,
    max_encoded_message_size,
    max_messages_per_batch,
    msgs.as_ref(),
  );

  let mut batches = SmallVec::with_capacity(hints.len());
  let mut msgs = msgs.into_iter();
  for hint in hints {
    match hint {
      BatchHint::One { encoded_size, .. } => {
        batches.push(Batch::One {
          msg: msgs.next().unwrap(),
          estimate_encoded_size: encoded_size,
        });
      }
      BatchHint::More {
        range,
        encoded_size,
      } => {
        let mut batch = TinyVec::with_capacity(range.end - range.start);
        for _ in range {
          batch.push(msgs.next().unwrap());
        }
        batches.push(Batch::More {
          estimate_encoded_size: encoded_size,
          msgs: batch,
        });
      }
    }
  }
  batches
}

#[test]
fn test_batch() {
  use crate::transport::{Lpe, Wire};
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  let single = Message::<SmolStr, SocketAddr>::UserData("ping".into());
  let encoded_len = Lpe::<_, _>::encoded_len(&single);
  let batches = batch::<_, _, _, Lpe<_, _>>(0, 2, 2, 1400, u16::MAX as usize, 255, SmallVec::from(single));
  assert_eq!(batches.len(), 1, "bad len {}", batches.len());
  assert_eq!(batches[0].estimate_encoded_size(), encoded_len, "bad estimate len");

  let mut total_encoded_len = 0;
  let bcasts = (0..256)
    .map(|i| {
      let msg = Message::UserData(i.to_string().as_bytes().to_vec().into());
      let encoded_len = Lpe::<_, _>::encoded_len(&msg);
      total_encoded_len += 2 + encoded_len;
      msg
    })
    .collect::<SmallVec<Message<SmolStr, SocketAddr>>>();

  let batches = batch::<_, _, _, Lpe<_, _>>(0, 2, 2, 1400, u16::MAX as usize, 255, bcasts);
  assert_eq!(batches.len(), 2, "bad len {}", batches.len());
  assert_eq!(batches[0].len() + batches[1].len(), 255, "missing packets");
  assert_eq!(batches[0].estimate_encoded_size() + batches[1].estimate_encoded_size(), total_encoded_len + 2 + 2, "bad estimate len");
}

#[test]
fn test_retransmit_limit() {
  let lim = retransmit_limit(3, 0);
  assert_eq!(lim, 0, "bad val {lim}");

  let lim = retransmit_limit(3, 1);
  assert_eq!(lim, 3, "bad val {lim}");

  let lim = retransmit_limit(3, 99);
  assert_eq!(lim, 6, "bad val {lim}");
}
