use std::{sync::Arc, time::Duration};

use async_lock::RwLock;

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
  pub(crate) inner: Arc<RwLock<Inner>>,
  #[cfg(feature = "metrics")]
  pub(crate) metric_labels: Arc<Vec<metrics::Label>>,
}

impl Awareness {
  /// Returns a new awareness object.
  pub(crate) fn new(
    max: isize,
    #[cfg(feature = "metrics")] metric_labels: Arc<Vec<metrics::Label>>,
  ) -> Self {
    Self {
      inner: Arc::new(RwLock::new(Inner { max, score: 0 })),
      #[cfg(feature = "metrics")]
      metric_labels,
    }
  }

  /// Takes the given delta and applies it to the score in a thread-safe
  /// manner. It also enforces a floor of zero and a max of max, so deltas may not
  /// change the overall score if it's railed at one of the extremes.
  pub(crate) async fn apply_delta(&self, delta: isize) {
    let (_initial, _fnl) = {
      let mut inner = self.inner.write().await;
      let initial = inner.score;
      inner.score += delta;
      if inner.score < 0 {
        inner.score = 0;
      } else if inner.score > inner.max - 1 {
        inner.score = inner.max - 1;
      }
      (initial, inner.score)
    };

    #[cfg(feature = "metrics")]
    {
      if _initial != _fnl {
        metrics::gauge!("memberlist.health.score", self.metric_labels.iter()).set(_fnl as f64);
      }
    }
  }

  /// Returns the raw health score.
  pub(crate) async fn get_health_score(&self) -> isize {
    self.inner.read().await.score
  }

  /// Takes the given duration and scales it based on the current
  /// score. Less healthyness will lead to longer timeouts.
  pub(crate) async fn scale_timeout(&self, timeout: Duration) -> Duration {
    let score = self.inner.read().await.score;
    timeout * ((score + 1) as u32)
  }
}

#[tokio::test]
#[cfg(test)]
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
  let a = Awareness::new(
    8,
    #[cfg(feature = "metrics")]
    Arc::new(vec![]),
  );
  for (delta, score, timeout) in cases {
    a.apply_delta(delta).await;
    assert_eq!(a.get_health_score().await, score);
    assert_eq!(a.scale_timeout(Duration::from_secs(1)).await, timeout);
  }
}
