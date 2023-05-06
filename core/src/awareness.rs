use std::{sync::Arc, time::Duration};

use metrics::Label;

#[derive(Debug)]
pub(crate) struct Inner {
  /// max is the upper threshold for the timeout scale (the score will be
  /// constrained to be from 0 <= score < max).
  max: isize,
  /// score is the current awareness score. Lower values are healthier and
  /// zero is the minimum value.
  score: isize,
}

/// Manages a simple metric for tracking the estimated health of the
/// local node. Health is primary the node's ability to respond in the soft
/// real-time manner required for correct health checking of other nodes in the
/// cluster.
#[derive(Debug, Clone)]
pub(crate) struct Awareness {
  #[cfg(feature = "async")]
  pub(crate) inner: Arc<async_lock::RwLock<Inner>>,
  #[cfg(not(feature = "async"))]
  pub(crate) inner: Arc<parking_lot::RwLock<Inner>>,
  pub(crate) metric_labels: Arc<Vec<Label>>,
}

impl Awareness {
  /// Returns a new awareness object.
  pub(crate) fn new(max: isize, metric_labels: Arc<Vec<Label>>) -> Self {
    Self {
      #[cfg(feature = "async")]
      inner: Arc::new(async_lock::RwLock::new(Inner { max, score: 0 })),
      #[cfg(not(feature = "async"))]
      inner: Arc::new(parking_lot::RwLock::new(Inner { max, score: 0 })),
      metric_labels,
    }
  }

  /// Takes the given delta and applies it to the score in a thread-safe
  /// manner. It also enforces a floor of zero and a max of max, so deltas may not
  /// change the overall score if it's railed at one of the extremes.
  #[cfg(feature = "async")]
  pub(crate) async fn apply_delta(&self, delta: isize) {
    let mut inner = self.inner.write().await;
    let initial = inner.score;
    inner.score += delta;
    if inner.score < 0 {
      inner.score = 0;
    } else if inner.score > inner.max - 1 {
      inner.score = inner.max - 1;
    }

    let fnl = inner.score;
    drop(inner);
    if initial != fnl {
      // TODO: update metrics
    }
  }

  #[cfg(not(feature = "async"))]
  pub(crate) fn apply_delta(&self, delta: isize) {
    let mut inner = self.inner.write();
    let initial = inner.score;
    inner.score += delta;
    if inner.score < 0 {
      inner.score = 0;
    } else if inner.score > inner.max - 1 {
      inner.score = inner.max - 1;
    }

    let fnl = inner.score;
    drop(inner);
    if initial != fnl {
      // TODO: update metrics
    }
  }

  /// Returns the raw health score.
  #[cfg(feature = "async")]
  pub(crate) async fn get_health_score(&self) -> isize {
    self.inner.read().await.score
  }

  #[cfg(not(feature = "async"))]
  pub(crate) fn get_health_score(&self) -> isize {
    self.inner.read().score
  }

  /// Takes the given duration and scales it based on the current
  /// score. Less healthyness will lead to longer timeouts.
  #[cfg(feature = "async")]
  pub(crate) async fn scale_timeout(&self, timeout: Duration) -> Duration {
    let score = self.inner.read().await.score;
    timeout * ((score + 1) as u32)
  }

  #[cfg(not(feature = "async"))]
  pub(crate) fn scale_timeout(&self, timeout: Duration) -> Duration {
    let score = self.inner.read().score;
    timeout * ((score + 1) as u32)
  }
}

#[tokio::test]
#[cfg(all(feature = "async", test))]
async fn test_awareness() {
  let cases = vec![
    (0, 0, Duration::from_secs(1)),
    (-1, 0, Duration::from_secs(1)),
    (-10, 0, Duration::from_secs(1)),
    (1, 1, Duration::from_secs(2)),
    (-1, 0, Duration::from_secs(1)),
    (10, 7, Duration::from_secs(8)),
    (-1, 6, Duration::from_secs(7)),
    (-1, 5, Duration::from_secs(6)),
    (-1, 4, Duration::from_secs(5)),
    (-1, 3, Duration::from_secs(4)),
    (-1, 2, Duration::from_secs(3)),
    (-1, 1, Duration::from_secs(2)),
    (-1, 0, Duration::from_secs(1)),
    (-1, 0, Duration::from_secs(1)),
  ];

  let a = Awareness::new(8, Arc::new(vec![]));
  for (delta, score, timeout) in cases {
    a.apply_delta(delta).await;
    assert_eq!(a.get_health_score().await, score);
    assert_eq!(a.scale_timeout(Duration::from_secs(1)).await, timeout);
  }
}

#[test]
#[cfg(all(not(feature = "async"), test))]
fn test_awareness() {
  let cases = vec![
    (0, 0, Duration::from_secs(1)),
    (-1, 0, Duration::from_secs(1)),
    (-10, 0, Duration::from_secs(1)),
    (1, 1, Duration::from_secs(2)),
    (-1, 0, Duration::from_secs(1)),
    (10, 7, Duration::from_secs(8)),
    (-1, 6, Duration::from_secs(7)),
    (-1, 5, Duration::from_secs(6)),
    (-1, 4, Duration::from_secs(5)),
    (-1, 3, Duration::from_secs(4)),
    (-1, 2, Duration::from_secs(3)),
    (-1, 1, Duration::from_secs(2)),
    (-1, 0, Duration::from_secs(1)),
    (-1, 0, Duration::from_secs(1)),
  ];

  let a = Awareness::new(8, Arc::new(vec![]));
  for (delta, score, timeout) in cases {
    a.apply_delta(delta);
    assert_eq!(a.get_health_score(), score);
    assert_eq!(a.scale_timeout(Duration::from_secs(1)), timeout);
  }
}
