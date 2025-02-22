use std::{net::SocketAddr, sync::Arc, time::Duration};

#[cfg(any(test, feature = "test"))]
use std::sync::atomic::Ordering;

use agnostic::RuntimeLite;
use memberlist_core::transport::{StreamProducer, Transport};
use nodecraft::resolver::AddressResolver;

use super::{Listener, StreamLayer};

#[allow(dead_code)]
#[cfg(any(test, feature = "test"))]
static BACKOFFS_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

#[cfg(any(test, feature = "test"))]
static BACKOFFS_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub(super) struct PromisedProcessor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<
    Resolver = A,
    ResolvedAddress = SocketAddr,
    Connection = S::Stream,
    Runtime = A::Runtime,
  >,
  S: StreamLayer<Runtime = A::Runtime>,
{
  pub(super) stream_tx:
    StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, S::Stream>,
  pub(super) ln: Arc<S::Listener>,
  pub(super) local_addr: SocketAddr,
  pub(super) shutdown_rx: async_channel::Receiver<()>,
}

impl<A, T, S> PromisedProcessor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<
    Resolver = A,
    ResolvedAddress = SocketAddr,
    Connection = S::Stream,
    Runtime = A::Runtime,
  >,
  S: StreamLayer<Runtime = A::Runtime>,
{
  pub(super) async fn run(self) {
    let Self {
      stream_tx,
      ln,
      local_addr,
      shutdown_rx,
    } = self;

    /// The initial delay after an `accept()` error before attempting again
    const BASE_DELAY: Duration = Duration::from_millis(5);

    /// the maximum delay after an `accept()` error before attempting again.
    /// In the case that tcpListen() is error-looping, it will delay the shutdown check.
    /// Therefore, changes to `MAX_DELAY` may have an effect on the latency of shutdown.
    const MAX_DELAY: Duration = Duration::from_secs(1);

    let mut loop_delay = Duration::ZERO;
    loop {
      let fut1 = shutdown_rx.recv();
      let fut2 = async {
        match ln.accept().await {
          Ok((conn, remote_addr)) => {
            // No error, reset loop delay
            loop_delay = Duration::ZERO;
            if let Err(e) = stream_tx.send(remote_addr, conn).await {
              tracing::error!(local_addr=%local_addr, err = %e, "memberlist.transport.net: failed to send TCP connection");
            }
            true
          }
          Err(e) => {
            if shutdown_rx.is_closed() {
              return false;
            }

            #[cfg(any(test, feature = "test"))]
            {
              BACKOFFS_COUNT.fetch_add(1, Ordering::SeqCst);
            }

            if loop_delay == Duration::ZERO {
              loop_delay = BASE_DELAY;
            } else {
              loop_delay *= 2;
            }

            if loop_delay > MAX_DELAY {
              loop_delay = MAX_DELAY;
            }

            tracing::error!(local_addr=%local_addr, err = %e, "memberlist.transport.net: error accepting TCP connection");
            <T::Runtime as RuntimeLite>::sleep(loop_delay).await;
            true
          }
        }
      };

      futures::pin_mut!(fut1, fut2);

      match futures::future::select(fut1, fut2).await {
        futures::future::Either::Left((_, _)) => break,
        futures::future::Either::Right((r, _)) => {
          if r {
            continue;
          }
          break;
        }
      }
    }
    let _ = ln.shutdown().await;
    tracing::info!(
      "memberlist.transport.net: promised processor on {} exit",
      local_addr
    );
  }
}

// /// Unit test for testing promised processor backoff
// #[cfg(any(feature = "test", test))]
// #[cfg_attr(docsrs, doc(cfg(any(feature = "test", test))))]
// #[allow(clippy::await_holding_lock, dead_code)]
// pub async fn listener_backoff<A, T, S>(
//   s: S,
//   kind: memberlist_core::transport::tests::AddressKind,
// ) -> Result<(), memberlist_core::tests::AnyError>
// where
//   A: AddressResolver<ResolvedAddress = SocketAddr>,
//   T:
//     Transport<Resolver = A, ResolvedAddress = SocketAddr, Connection = S::Stream, Runtime = A::Runtime>,
//   S: StreamLayer<Runtime = A::Runtime>,
// {
//   struct TestStreamLayer<TS: StreamLayer> {
//     _m: std::marker::PhantomData<TS>,
//   }

//   struct TestListener<TS: StreamLayer> {
//     ln: TS::Listener,
//   }

//   impl<TS: StreamLayer> Listener for TestListener<TS> {
//     type Stream = TS::Stream;

//     async fn accept(&self) -> std::io::Result<(Self::Stream, SocketAddr)> {
//       Err(std::io::Error::new(
//         std::io::ErrorKind::Other,
//         "always fail to accept for testing",
//       ))
//     }

//     fn local_addr(&self) -> SocketAddr {
//       self.ln.local_addr()
//     }

//     async fn shutdown(&self) -> std::io::Result<()> {
//       self.ln.shutdown().await
//     }
//   }

//   impl<TS: StreamLayer> StreamLayer for TestStreamLayer<TS> {
//     type Runtime = TS::Runtime;
//     type Listener = TestListener<TS>;
//     type Stream = TS::Stream;
//     type Options = ();

//     async fn new(_: Self::Options) -> std::io::Result<Self>
//     where
//       Self: Sized,
//     {
//       unreachable!()
//     }

//     async fn connect(&self, _addr: SocketAddr) -> std::io::Result<Self::Stream> {
//       unreachable!()
//     }

//     async fn bind(&self, _addr: SocketAddr) -> std::io::Result<Self::Listener> {
//       unreachable!()
//     }

//     fn is_secure() -> bool {
//       unreachable!()
//     }
//   }

//   const TEST_TIME: std::time::Duration = std::time::Duration::from_secs(4);

//   let _lock = BACKOFFS_LOCK.lock();
//   let ln = s.bind(kind.next(0)).await?;
//   let local_addr = ln.local_addr();
//   let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
//   let (stream_tx, stream_rx) = memberlist_core::transport::promised_stream::<T>();
//   let task = <T::Runtime as RuntimeLite>::spawn(
//     PromisedProcessor::<A, T, TestStreamLayer<S>> {
//       stream_tx,
//       ln: Arc::new(TestListener { ln }),
//       local_addr,
//       shutdown_rx,
//     }
//     .run(),
//   );

//   // sleep (+yield) for testTime seconds before asking the accept loop to shut down
//   <T::Runtime as RuntimeLite>::sleep(TEST_TIME).await;
//   shutdown_tx.close();
//   // Verify that the wg was completed on exit (but without blocking this test)
//   // maxDelay == 1s, so we will give the routine 1.25s to loop around and shut down.
//   let (ctx, crx) = async_channel::bounded::<()>(1);
//   <T::Runtime as RuntimeLite>::spawn_detach(async move {
//     let _ = task.await;
//     ctx.close();
//   });

//   futures::select! {
//     _ = crx.recv().fuse() => {}
//     _ = <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(1250)).fuse() => {
//       panic!("timed out waiting for transport waitgroup to be done after flagging shutdown");
//     }
//   }

//   // In testTime==4s, we expect to loop approximately 12 times (and log approximately 11 errors),
//   // with the following delays (in ms):
//   //   0+5+10+20+40+80+160+320+640+1000+1000+1000 == 4275 ms
//   // Too few calls suggests that the minDelay is not in force; too many calls suggests that the
//   // maxDelay is not in force or that the back-off isn't working at all.
//   // We'll leave a little flex; the important thing here is the asymptotic behavior.
//   // If the minDelay or maxDelay in NetTransport#tcpListen() are modified, this test may fail
//   // and need to be adjusted.
//   let calls = BACKOFFS_COUNT.load(Ordering::SeqCst);
//   assert!(8 < calls && calls < 14, "calls: {calls}");

//   // no connections should have been accepted and sent to the channel
//   assert!(stream_rx.is_empty());
//   BACKOFFS_COUNT.store(0, Ordering::SeqCst);

//   Ok(())
// }
