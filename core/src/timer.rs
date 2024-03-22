// use agnostic_lite::RuntimeLite;
// use futures::{channel::oneshot, future::FutureExt, select};
// use std::time::Duration;

// pub(crate) struct Timer<R> {
//   #[allow(dead_code)]
//   fut: oneshot::Receiver<()>,
//   cancel: oneshot::Sender<()>,
//   _m: std::marker::PhantomData<R>,
// }

// impl<R> Timer<R>
// where
//   R: RuntimeLite,
// {
//   pub(crate) fn after<F>(dur: Duration, future: F) -> Self
//   where
//     F: std::future::Future<Output = ()> + Send + 'static,
//   {
//     let (cancel_tx, cancel_rx) = oneshot::channel();
//     let (tx, rx) = oneshot::channel();
//     R::spawn_detach(
//       async move {
//         let delay = R::sleep(dur);
//         select! {
//           _ = delay.fuse() => {
//             future.await;
//             let _ = tx.send(());
//           },
//           _ = cancel_rx.fuse() => {},
//         }
//       }
//     );

//     Timer {
//       fut: rx,
//       cancel: cancel_tx,
//       _m: std::marker::PhantomData,
//     }
//   }

//   pub(crate) async fn stop(self) {
//     let _ = self.cancel.send(());
//   }
// }
