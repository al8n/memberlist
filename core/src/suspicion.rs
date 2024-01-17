use std::{
  collections::HashMap,
  sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

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

use nodecraft::Node;

use super::*;
use agnostic::Runtime;
use futures::{future::BoxFuture, Future, FutureExt};

pub(crate) struct Suspicion<I, A, R> {
  n: Arc<AtomicU32>,
  k: u32,
  min: Duration,
  max: Duration,
  start: Instant,
  after_func: AfterFunc<R>,
  confirmations: HashMap<I, A>,
}

impl<I, A, R> Suspicion<I, A, R>
where
  I: Eq + core::hash::Hash,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  /// Returns a after_func started with the max time, and that will drive
  /// to the min time after seeing k or more confirmations. The from node will be
  /// excluded from confirmations since we might get our own suspicion message
  /// gossiped back to us. The minimum time will be used if no confirmations are
  /// called for (k = 0).
  pub(crate) fn new(
    from: Node<I, A>,
    k: u32,
    min: Duration,
    max: Duration,
    timeout_fn: impl Fn(u32) -> BoxFuture<'static, ()> + Clone + Send + Sync + 'static,
  ) -> Self {
    #[allow(clippy::mutable_key_type)]
    let confirmations = [from].into_iter().map(|n| n.into_components()).collect();
    let n = Arc::new(AtomicU32::new(0));
    let timeout = if k < 1 { min } else { max };
    let after_func = AfterFunc::new(n.clone(), timeout, Arc::new(timeout_fn));
    after_func.start();
    Suspicion {
      n,
      k,
      min,
      max,
      start: Instant::now(),
      after_func,
      confirmations,
    }
  }
}

impl<I, A, R> Suspicion<I, A, R>
where
  I: Clone + Eq + core::hash::Hash,
  A: Clone,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  /// Confirm registers that a possibly new peer has also determined the given
  /// node is suspect. This returns true if this was new information, and false
  /// if it was a duplicate confirmation, or if we've got enough confirmations to
  /// hit the minimum.
  pub(crate) async fn confirm(&mut self, from: &Node<I, A>) -> bool {
    if self.n.load(Ordering::Relaxed) >= self.k {
      return false;
    }

    if self.confirmations.contains_key(from.id()) {
      return false;
    }
    let (id, addr) = from.clone().into_components();
    self.confirmations.insert(id, addr);

    // Compute the new timeout given the current number of confirmations and
    // adjust the after_func. If the timeout becomes negative *and* we can cleanly
    // stop the after_func then we will call the timeout function directly from
    // here.
    let n = self.n.fetch_add(1, Ordering::SeqCst) + 1;
    let elapsed = self.start.elapsed();
    let remaining = remaining_suspicion_time(n, self.k, elapsed, self.min, self.max);

    if self.after_func.stop().await {
      if remaining > Duration::ZERO {
        self.after_func.reset(remaining).await;
      } else {
        let n = self.n.clone();
        let fut = (self.after_func.f)(n.load(Ordering::SeqCst));
        R::spawn_detach(async move {
          fut.await;
        });
      }
    }
    true
  }
}

pub(super) struct AfterFunc<R> {
  n: Arc<AtomicU32>,
  timeout: Duration,
  start: Instant,
  stop_rx: async_channel::Receiver<()>,
  stop_tx: async_channel::Sender<()>,
  stopped: Arc<AtomicBool>,
  f: Arc<dyn Fn(u32) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
  _marker: std::marker::PhantomData<R>,
}

impl<R> AfterFunc<R> {
  pub fn new(
    n: Arc<AtomicU32>,
    timeout: Duration,
    f: Arc<impl Fn(u32) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
  ) -> Self {
    let (tx, rx) = async_channel::bounded(1);
    let stopped = Arc::new(AtomicBool::new(false));

    Self {
      n,
      timeout,
      stop_rx: rx,
      stop_tx: tx,
      stopped,
      f,
      start: Instant::now(),
      _marker: std::marker::PhantomData,
    }
  }

  pub async fn stop(&self) -> bool {
    if self.start.elapsed() >= self.timeout {
      return false;
    }
    if self.stopped.load(Ordering::SeqCst) {
      false
    } else {
      self.stopped.store(true, Ordering::SeqCst);
      let _ = self.stop_tx.send(()).await;
      true
    }
  }
}

impl<R> AfterFunc<R>
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  pub fn start(&self) {
    let rx = self.stop_rx.clone();
    let n = self.n.clone();
    let f = self.f.clone();
    let timeout = self.timeout;

    R::spawn_detach(async move {
      futures::select_biased! {
        _ = rx.recv().fuse() => {}
        _ = R::sleep(timeout).fuse() => {
          f(n.load(Ordering::SeqCst)).await
        }
      }
    });
  }

  pub async fn reset(&mut self, remaining: Duration) {
    self.stop().await;
    let rx = self.stop_rx.clone();
    let n = self.n.clone();
    let f = self.f.clone();
    self.timeout = remaining;
    self.start = Instant::now();

    R::spawn_detach(async move {
      futures::select_biased! {
        _ = rx.recv().fuse() => {}
        _ = R::sleep(remaining).fuse() => {
          f(n.load(Ordering::SeqCst)).await
        }
      }
    });
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

  // #[tokio::test]
  // async fn test_suspicion_after_func() {
  //   use agnostic::tokio::TokioRuntime;

  //   for (i, (num_confirmations, from, confirmations, expected)) in
  //     test_cases().into_iter().enumerate()
  //   {
  //     let (tx, rx) = async_channel::unbounded();
  //     let start = Instant::now();
  //     let f = move |nc: u32| {
  //       let tx = tx.clone();
  //       assert_eq!(
  //         nc, num_confirmations as u32,
  //         "case {}: bad {} != {}",
  //         i, nc, num_confirmations
  //       );
  //       async move {
  //         tx.send(Instant::now().duration_since(start)).await.unwrap();
  //       }
  //       .boxed()
  //     };

  //     let mut s = Suspicion::<TokioRuntime>::new(from.try_into().unwrap(), K, MIN, MAX, f);
  //     let fudge = Duration::from_millis(25);
  //     for p in confirmations.iter() {
  //       tokio::time::sleep(fudge).await;
  //       assert_eq!(
  //         s.confirm(&p.from.try_into().unwrap()).await,
  //         p.new_info,
  //         "case {}: newInfo mismatch for {}",
  //         i,
  //         p.from
  //       );
  //     }

  //     let sleep = expected
  //       - Duration::from_millis(confirmations.len() as u64 * (fudge.as_millis() as u64))
  //       - fudge;
  //     tokio::time::sleep(sleep).await;
  //     match rx.try_recv() {
  //       Ok(d) => panic!("case {}: should not have fired ({:?})", i, d),
  //       Err(TryRecvError::Empty) => (),
  //       Err(TryRecvError::Closed) => panic!("case {}: channel closed", i),
  //     }

  //     // Wait through the timeout and a little after and make sure it
  //     // fires.
  //     tokio::time::sleep(2 * fudge).await;
  //     match rx.recv().await {
  //       Ok(_) => (),
  //       Err(_) => panic!("case {}: should have fired", i),
  //     }

  //     // Confirm after to make sure it handles a negative remaining
  //     // time correctly and doesn't fire again.
  //     s.confirm(&"late".try_into().unwrap()).await;
  //     tokio::time::sleep(expected + 2 * fudge).await;
  //     match rx.try_recv() {
  //       Ok(d) => panic!("case {}: should not have fired ({:?})", i, d),
  //       Err(TryRecvError::Empty) => (),
  //       Err(TryRecvError::Closed) => panic!("case {}: channel closed", i),
  //     }
  //   }
  // }

  // #[tokio::test]
  // async fn test_suspicion_after_func_zero_k() {
  //   use agnostic::tokio::TokioRuntime;

  //   let (tx, rx) = async_channel::unbounded();
  //   let f = move |_| {
  //     let tx = tx.clone();
  //     async move {
  //       tx.send(()).await.unwrap();
  //     }
  //     .boxed()
  //   };

  //   let mut s = Suspicion::<TokioRuntime>::new(
  //     "me".try_into().unwrap(),
  //     0,
  //     Duration::from_millis(25),
  //     Duration::from_secs(30),
  //     f,
  //   );

  //   assert!(!s.confirm(&"foo".try_into().unwrap()).await);
  //   let _ = tokio::time::timeout(Duration::from_millis(50), rx.recv())
  //     .await
  //     .unwrap();
  // }

  // #[tokio::test]
  // async fn test_suspicion_after_func_immediate() {
  //   use agnostic::tokio::TokioRuntime;

  //   let (tx, rx) = async_channel::unbounded();
  //   let f = move |_| {
  //     let tx = tx.clone();
  //     async move {
  //       tx.send(()).await.unwrap();
  //     }
  //     .boxed()
  //   };

  //   let mut s = Suspicion::<TokioRuntime>::new(
  //     "me".try_into().unwrap(),
  //     1,
  //     Duration::from_millis(100),
  //     Duration::from_secs(30),
  //     f,
  //   );

  //   tokio::time::sleep(Duration::from_millis(200)).await;
  //   s.confirm(&"foo".try_into().unwrap()).await;

  //   let _ = tokio::time::timeout(Duration::from_millis(25), rx.recv())
  //     .await
  //     .unwrap();
  // }
}
