use agnostic::Runtime;
use futures::{channel::oneshot, future::FutureExt, select, Future};
use std::time::Duration;

pub(crate) struct Timer {
  #[allow(dead_code)]
  fut: oneshot::Receiver<()>,
  cancel: oneshot::Sender<()>,
}

impl Timer {
  pub(crate) fn after<F, R>(dur: Duration, future: F) -> Self
  where
    R: Runtime,
    <R::Sleep as Future>::Output: Send,
    F: std::future::Future<Output = ()> + Send + 'static,
  {
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let (tx, rx) = oneshot::channel();
    R::spawn_detach(
      async move {
        let delay = R::sleep(dur);
        select! {
          _ = delay.fuse() => {
            future.await;
            let _ = tx.send(());
          },
          _ = cancel_rx.fuse() => {},
        }
      }
      .boxed(),
    );

    Timer {
      fut: rx,
      cancel: cancel_tx,
    }
  }

  pub(crate) async fn stop(self) {
    let _ = self.cancel.send(());
  }
}
