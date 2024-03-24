use std::{
  collections::HashSet,
  sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use super::*;
use agnostic_lite::{AfterHandle, AsyncAfterSpawner, RuntimeLite};
use futures::future::BoxFuture;

#[inline]
fn remaining_suspicion_time(
  n: u32,
  k: u32,
  elapsed: Duration,
  min: Duration,
  max: Duration,
) -> Duration {
  let frac = (n as f64 + 1.0).ln() / (k as f64 + 1.0).ln();
  let raw = max.as_secs_f64() - frac * (max.as_secs_f64() - min.as_secs_f64());
  let timeout = (raw * 1000.0).floor();
  if timeout < min.as_millis() as f64 {
    min.saturating_sub(elapsed)
  } else {
    Duration::from_millis(timeout as u64).saturating_sub(elapsed)
  }
}

pub(crate) struct Suspicion<I, R: RuntimeLite> {
  n: Arc<AtomicU32>,
  k: u32,
  min: Duration,
  max: Duration,
  start: Instant,
  timeout_fn: Arc<dyn Fn(u32) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
  handle: Option<<R::AfterSpawner as AsyncAfterSpawner>::JoinHandle<()>>,
  confirmations: HashSet<I>,
}

impl<I, R> Suspicion<I, R>
where
  I: Eq + core::hash::Hash,
  R: RuntimeLite,
{
  /// Returns a after_func started with the max time, and that will drive
  /// to the min time after seeing k or more confirmations. The from node will be
  /// excluded from confirmations since we might get our own suspicion message
  /// gossiped back to us. The minimum time will be used if no confirmations are
  /// called for (k = 0).
  pub(crate) fn new(
    from: I,
    k: u32,
    min: Duration,
    max: Duration,
    timeout_fn: impl Fn(u32) -> BoxFuture<'static, ()> + Clone + Send + Sync + 'static,
  ) -> Self {
    #[allow(clippy::mutable_key_type)]
    let confirmations = [from].into_iter().collect();
    let n = Arc::new(AtomicU32::new(0));
    let timeout = if k < 1 { min } else { max };
    let timeout_fn = Arc::new(timeout_fn);
    let n1 = n.clone();
    let timeout_fn1 = timeout_fn.clone();
    let handle = R::spawn_after(timeout, async move {
      (timeout_fn1)(n1.load(Ordering::SeqCst)).await;
    });
    Suspicion {
      n,
      k,
      min,
      max,
      start: Instant::now(),
      timeout_fn,
      handle: Some(handle),
      confirmations,
    }
  }
}

impl<I, R> Suspicion<I, R>
where
  I: CheapClone + Eq + core::hash::Hash,
  R: RuntimeLite,
{
  /// Confirm registers that a possibly new peer has also determined the given
  /// node is suspect. This returns true if this was new information, and false
  /// if it was a duplicate confirmation, or if we've got enough confirmations to
  /// hit the minimum.
  pub(crate) async fn confirm(&mut self, from: &I) -> bool {
    if self.n.load(Ordering::Relaxed) >= self.k {
      return false;
    }

    if self.confirmations.contains(from) {
      return false;
    }

    self.confirmations.insert(from.cheap_clone());

    if let Some(h) = self.handle.take() {
      if !h.is_expired() {
        // Compute the new timeout given the current number of confirmations and
        // adjust the after_func. If the timeout becomes negative *and* we can cleanly
        // stop the after_func then we will call the timeout function directly from
        // here.
        let n = self.n.fetch_add(1, Ordering::SeqCst) + 1;
        let elapsed = self.start.elapsed();
        let remaining = remaining_suspicion_time(n, self.k, elapsed, self.min, self.max);

        if remaining > Duration::ZERO {
          h.abort();
          let n = self.n.clone();
          let timeout_fn = self.timeout_fn.clone();
          self.handle = Some(R::spawn_after_at(Instant::now() + remaining, async move {
            (timeout_fn)(n.load(Ordering::SeqCst)).await;
          }));
        } else {
          let n = self.n.clone();
          let timeout_fn = self.timeout_fn.clone();
          R::spawn_detach(async move {
            (timeout_fn)(n.load(Ordering::SeqCst)).await;
          });
        }
      }
    }

    true
  }
}

impl<I, R: RuntimeLite> Drop for Suspicion<I, R> {
  fn drop(&mut self) {
    if let Some(h) = self.handle.take() {
      h.abort();
    }
  }
}

#[test]
fn test_suspicion_remaining_suspicion_time() {
  let cases = vec![
    (
      0,
      3,
      Duration::from_secs(0),
      Duration::from_secs(2),
      Duration::from_secs(30),
      Duration::from_secs(30),
    ),
    (
      1,
      3,
      Duration::from_secs(2),
      Duration::from_secs(2),
      Duration::from_secs(30),
      Duration::from_secs(14),
    ),
    (
      2,
      3,
      Duration::from_secs(3),
      Duration::from_secs(2),
      Duration::from_secs(30),
      Duration::from_millis(4810),
    ),
    (
      3,
      3,
      Duration::from_secs(4),
      Duration::from_secs(2),
      Duration::from_secs(30),
      Duration::ZERO,
    ),
    (
      4,
      3,
      Duration::from_secs(5),
      Duration::from_secs(2),
      Duration::from_secs(30),
      Duration::ZERO,
    ),
    (
      5,
      3,
      Duration::from_secs(10),
      Duration::from_secs(2),
      Duration::from_secs(30),
      Duration::ZERO,
    ),
  ];

  for (i, (n, k, elapsed, min, max, expected)) in cases.into_iter().enumerate() {
    let remaining = remaining_suspicion_time(n, k, elapsed, min, max);
    assert_eq!(
      remaining, expected,
      "case {}: remaining {:?} != expected {:?}",
      i, remaining, expected
    );
  }
}

#[cfg(test)]
mod tests {
  use async_channel::TryRecvError;
  use smol_str::SmolStr;

  use super::*;

  struct Pair {
    from: &'static str,
    new_info: bool,
  }

  const K: u32 = 3;
  const MIN: Duration = Duration::from_millis(500);
  const MAX: Duration = Duration::from_secs(2);

  fn test_cases() -> Vec<(i32, &'static str, Vec<Pair>, Duration)> {
    vec![
      (0, "me", vec![], MAX),
      (
        1,
        "me",
        vec![
          Pair {
            from: "me",
            new_info: false,
          },
          Pair {
            from: "foo",
            new_info: true,
          },
        ],
        Duration::from_millis(1250),
      ),
      (
        1,
        "me",
        vec![
          Pair {
            from: "me",
            new_info: false,
          },
          Pair {
            from: "foo",
            new_info: true,
          },
          Pair {
            from: "foo",
            new_info: false,
          },
          Pair {
            from: "foo",
            new_info: false,
          },
        ],
        Duration::from_millis(1250),
      ),
      (
        2,
        "me",
        vec![
          Pair {
            from: "me",
            new_info: false,
          },
          Pair {
            from: "foo",
            new_info: true,
          },
          Pair {
            from: "bar",
            new_info: true,
          },
        ],
        Duration::from_millis(810),
      ),
      (
        3,
        "me",
        vec![
          Pair {
            from: "me",
            new_info: false,
          },
          Pair {
            from: "foo",
            new_info: true,
          },
          Pair {
            from: "bar",
            new_info: true,
          },
          Pair {
            from: "baz",
            new_info: true,
          },
        ],
        MIN,
      ),
      (
        3,
        "me",
        vec![
          Pair {
            from: "me",
            new_info: false,
          },
          Pair {
            from: "foo",
            new_info: true,
          },
          Pair {
            from: "bar",
            new_info: true,
          },
          Pair {
            from: "baz",
            new_info: true,
          },
          Pair {
            from: "zoo",
            new_info: false,
          },
        ],
        MIN,
      ),
    ]
  }

  #[tokio::test]
  async fn test_suspicion_after_func() {
    use agnostic::tokio::TokioRuntime;
    use futures::FutureExt;

    for (i, (num_confirmations, from, confirmations, expected)) in
      test_cases().into_iter().enumerate()
    {
      let (tx, rx) = async_channel::unbounded();
      let start = Instant::now();
      let f = move |nc: u32| {
        let tx = tx.clone();
        assert_eq!(
          nc, num_confirmations as u32,
          "case {}: bad {} != {}",
          i, nc, num_confirmations
        );
        async move {
          tx.send(Instant::now().duration_since(start)).await.unwrap();
        }
        .boxed()
      };

      let mut s = Suspicion::<SmolStr, TokioRuntime>::new(from.into(), K, MIN, MAX, f);
      let fudge = Duration::from_millis(25);
      for p in confirmations.iter() {
        tokio::time::sleep(fudge).await;
        assert_eq!(
          s.confirm(&p.from.into()).await,
          p.new_info,
          "case {}: newInfo mismatch for {}",
          i,
          p.from
        );
      }

      let sleep = expected - fudge * (confirmations.len() as u32) - fudge;
      tokio::time::sleep(sleep).await;
      match rx.try_recv() {
        Ok(d) => panic!("case {}: should not have fired ({:?})", i, d),
        Err(TryRecvError::Empty) => (),
        Err(TryRecvError::Closed) => panic!("case {}: channel closed", i),
      }

      // Wait through the timeout and a little after and make sure it
      // fires.
      tokio::time::sleep(2 * fudge).await;
      match rx.recv().await {
        Ok(_) => (),
        Err(_) => panic!("case {}: should have fired", i),
      }

      // Confirm after to make sure it handles a negative remaining
      // time correctly and doesn't fire again.
      s.confirm(&"late".into()).await;
      tokio::time::sleep(expected + 2 * fudge).await;
      match rx.try_recv() {
        Ok(d) => panic!("case {}: should not have fired ({:?})", i, d),
        Err(TryRecvError::Empty) => (),
        Err(TryRecvError::Closed) => panic!("case {}: channel closed", i),
      }
    }
  }

  #[tokio::test]
  async fn test_suspicion_after_func_zero_k() {
    use agnostic::tokio::TokioRuntime;
    use futures::FutureExt;

    let (tx, rx) = async_channel::unbounded();
    let f = move |_| {
      let tx = tx.clone();
      async move {
        tx.send(()).await.unwrap();
      }
      .boxed()
    };

    let mut s = Suspicion::<SmolStr, TokioRuntime>::new(
      "me".into(),
      0,
      Duration::from_millis(25),
      Duration::from_secs(30),
      f,
    );

    assert!(!s.confirm(&"foo".into()).await);
    let _ = tokio::time::timeout(Duration::from_millis(50), rx.recv())
      .await
      .unwrap();
  }

  #[tokio::test]
  async fn test_suspicion_after_func_immediate() {
    use agnostic::tokio::TokioRuntime;
    use futures::FutureExt;

    let (tx, rx) = async_channel::unbounded();
    let f = move |_| {
      let tx = tx.clone();
      async move {
        tx.send(()).await.unwrap();
      }
      .boxed()
    };

    let mut s: Suspicion<SmolStr, _> = Suspicion::<SmolStr, TokioRuntime>::new(
      "me".into(),
      1,
      Duration::from_millis(100),
      Duration::from_secs(30),
      f,
    );

    tokio::time::sleep(Duration::from_millis(200)).await;
    s.confirm(&"foo".into()).await;

    let _ = tokio::time::timeout(Duration::from_millis(25), rx.recv())
      .await
      .unwrap();
  }
}
