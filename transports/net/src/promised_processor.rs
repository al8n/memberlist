use std::{
  net::SocketAddr,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::Duration,
};

use agnostic::Runtime;
use futures::FutureExt;
use memberlist_core::transport::{stream::StreamProducer, Transport};
use nodecraft::resolver::AddressResolver;

use super::{Listener, StreamLayer};

#[cfg(any(test, feature = "test"))]
static BACKOFFS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
#[cfg(any(test, feature = "test"))]
static BACKOFFS_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub(super) struct PromisedProcessor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Stream = S::Stream, Runtime = A::Runtime>,
  S: StreamLayer,
{
  pub(super) stream_tx:
    StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, S::Stream>,
  pub(super) ln: Arc<S::Listener>,
  pub(super) local_addr: SocketAddr,
  pub(super) shutdown: Arc<AtomicBool>,
  pub(super) shutdown_rx: async_channel::Receiver<()>,
}

impl<A, T, S> PromisedProcessor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Stream = S::Stream, Runtime = A::Runtime>,
  S: StreamLayer,
{
  pub(super) async fn run(self) {
    let Self {
      stream_tx,
      ln,
      shutdown,
      local_addr,
      ..
    } = self;

    /// The initial delay after an `accept()` error before attempting again
    const BASE_DELAY: Duration = Duration::from_millis(5);

    /// the maximum delay after an `accept()` error before attempting again.
    /// In the case that tcpListen() is error-looping, it will delay the shutdown check.
    /// Therefore, changes to `MAX_DELAY` may have an effect on the latency of shutdown.
    const MAX_DELAY: Duration = Duration::from_secs(1);

    let mut loop_delay = Duration::ZERO;
    loop {
      futures::select! {
        _ = self.shutdown_rx.recv().fuse() => {
          break;
        }
        rst = ln.accept().fuse() => {
          match rst {
            Ok((conn, remote_addr)) => {
              // No error, reset loop delay
              loop_delay = Duration::ZERO;
              if let Err(e) = stream_tx
                .send(remote_addr, conn)
                .await
              {
                tracing::error!(target =  "memberlist.transport.net", local_addr=%local_addr, err = %e, "failed to send TCP connection");
              }
            }
            Err(e) => {
              if shutdown.load(Ordering::SeqCst) {
                break;
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

              tracing::error!(target =  "memberlist.transport.net", local_addr=%local_addr, err = %e, "error accepting TCP connection");
              <T::Runtime as Runtime>::sleep(loop_delay).await;
              continue;
            }
          }
        }
      }
    }
  }
}

/// Unit test for testing promised processor backoff
#[cfg(any(feature = "test", test))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "test", test))))]
#[allow(clippy::await_holding_lock)]
pub async fn listener_backoff<A, T, S>(
  s: S,
  kind: memberlist_core::transport::tests::AddressKind,
) -> Result<(), memberlist_core::tests::AnyError>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Stream = S::Stream, Runtime = A::Runtime>,
  S: StreamLayer,
{
  struct TestStreamLayer<TS: StreamLayer> {
    _m: std::marker::PhantomData<TS>,
  }

  struct TestListener<TS: StreamLayer> {
    ln: TS::Listener,
  }

  impl<TS: StreamLayer> Listener for TestListener<TS> {
    type Stream = TS::Stream;

    async fn accept(&self) -> std::io::Result<(Self::Stream, SocketAddr)> {
      Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        "always fail to accept for testing",
      ))
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
      self.ln.local_addr()
    }
  }

  impl<TS: StreamLayer> StreamLayer for TestStreamLayer<TS> {
    type Listener = TestListener<TS>;
    type Stream = TS::Stream;

    async fn connect(&self, _addr: SocketAddr) -> std::io::Result<Self::Stream> {
      unreachable!()
    }

    async fn bind(&self, _addr: SocketAddr) -> std::io::Result<Self::Listener> {
      unreachable!()
    }

    fn cache_stream(&self, _addr: SocketAddr, _stream: Self::Stream) {}

    fn is_secure() -> bool {
      unreachable!()
    }
  }

  const TEST_TIME: std::time::Duration = std::time::Duration::from_secs(4);

  let _lock = BACKOFFS_LOCK.lock().unwrap();
  let ln = s.bind(kind.next()).await?;
  let local_addr = ln.local_addr()?;
  let (_shutdown_tx, shutdown_rx) = async_channel::bounded(1);
  let shutdown = Arc::new(AtomicBool::new(false));
  let (stream_tx, stream_rx) = memberlist_core::transport::stream::promised_stream::<T>();
  let wg = agnostic::WaitableSpawner::<A::Runtime>::new();
  wg.spawn_detach(
    PromisedProcessor::<A, T, TestStreamLayer<S>> {
      stream_tx,
      ln: Arc::new(TestListener { ln }),
      local_addr,
      shutdown: shutdown.clone(),
      shutdown_rx,
    }
    .run(),
  );

  // sleep (+yield) for testTime seconds before asking the accept loop to shut down
  <T::Runtime as Runtime>::sleep(TEST_TIME).await;
  shutdown.store(true, Ordering::SeqCst);
  // Verify that the wg was completed on exit (but without blocking this test)
  // maxDelay == 1s, so we will give the routine 1.25s to loop around and shut down.
  let (ctx, crx) = async_channel::bounded::<()>(1);
  <T::Runtime as Runtime>::spawn_detach(async move {
    wg.wait().await;
    ctx.close();
  });

  futures::select! {
    _ = crx.recv().fuse() => {}
    _ = <T::Runtime as Runtime>::sleep(Duration::from_millis(1250)).fuse() => {
      panic!("timed out waiting for transport waitgroup to be done after flagging shutdown");
    }
  }

  // In testTime==4s, we expect to loop approximately 12 times (and log approximately 11 errors),
  // with the following delays (in ms):
  //   0+5+10+20+40+80+160+320+640+1000+1000+1000 == 4275 ms
  // Too few calls suggests that the minDelay is not in force; too many calls suggests that the
  // maxDelay is not in force or that the back-off isn't working at all.
  // We'll leave a little flex; the important thing here is the asymptotic behavior.
  // If the minDelay or maxDelay in NetTransport#tcpListen() are modified, this test may fail
  // and need to be adjusted.
  let calls = BACKOFFS_COUNT.load(Ordering::SeqCst);
  assert!(8 < calls && calls < 14);

  // no connections should have been accepted and sent to the channel
  assert!(stream_rx.is_empty());
  BACKOFFS_COUNT.store(0, Ordering::SeqCst);

  Ok(())
}
