use crate::transport::Transport;
use futures_timer::Delay;
use futures_util::{future::BoxFuture, select_biased, FutureExt};
use std::{future::Future, io, marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};
use trust_dns_proto::Time;
use trust_dns_resolver::{
  name_server::{RuntimeProvider, Spawn},
  AsyncResolver,
};

pub(crate) type DNS<T> = AsyncResolver<AsyncRuntimeProvider<T>>;

#[repr(transparent)]
pub(crate) struct AsyncSpawn {
  spawner: Arc<dyn Fn(BoxFuture<'static, ()>) + Send + Sync + 'static + Unpin>,
}

impl Clone for AsyncSpawn {
  fn clone(&self) -> Self {
    Self {
      spawner: self.spawner.clone(),
    }
  }
}

impl Spawn for AsyncSpawn {
  fn spawn_bg<F>(&mut self, future: F)
  where
    F: Future<Output = Result<(), trust_dns_proto::error::ProtoError>> + Send + 'static,
  {
    (self.spawner)(Box::pin(async move {
      if let Err(e) = future.await {
        tracing::error!(target = "showbiz", err = %e, "dns error");
      }
    }));
  }
}

pub(crate) struct AsyncRuntimeProvider<T: Transport> {
  spawner: AsyncSpawn,
  _marker: PhantomData<T>,
}

impl<T: Transport> AsyncRuntimeProvider<T> {
  pub(crate) fn new<S>(spawner: S) -> Self
  where
    S: Fn(BoxFuture<'static, ()>) + Send + Sync + 'static + Unpin,
  {
    Self {
      spawner: AsyncSpawn {
        spawner: Arc::new(spawner),
      },
      _marker: PhantomData,
    }
  }
}

impl<T> Clone for AsyncRuntimeProvider<T>
where
  T: Transport,
{
  fn clone(&self) -> Self {
    Self {
      spawner: self.spawner.clone(),
      _marker: PhantomData,
    }
  }
}

pub(crate) struct Timer;

#[async_trait::async_trait]
impl Time for Timer {
  /// Return a type that implements `Future` that will wait until the specified duration has
  /// elapsed.
  async fn delay_for(duration: Duration) {
    Delay::new(duration).await
  }

  /// Return a type that implement `Future` to complete before the specified duration has elapsed.
  async fn timeout<F: 'static + Future + Send>(
    duration: Duration,
    future: F,
  ) -> Result<F::Output, std::io::Error> {
    select_biased! {
      rst = future.fuse() => {
        return Ok(rst);
      }
      _ = Delay::new(duration).fuse() => {
        return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out"));
      }
    }
  }
}

impl<T: Transport + Unpin> RuntimeProvider for AsyncRuntimeProvider<T> {
  type Handle = AsyncSpawn;

  type Timer = Timer;

  type Udp = T::UnreliableConnection;

  type Tcp = T::Connection;

  fn create_handle(&self) -> Self::Handle {
    self.spawner.clone()
  }

  fn connect_tcp(
    &self,
    addr: SocketAddr,
  ) -> std::pin::Pin<Box<dyn Send + Future<Output = io::Result<Self::Tcp>>>> {
    Box::pin(<T as Transport>::connect(addr))
  }

  fn bind_udp(
    &self,
    local_addr: SocketAddr,
    _server_addr: SocketAddr,
  ) -> std::pin::Pin<Box<dyn Send + Future<Output = io::Result<Self::Udp>>>> {
    Box::pin(<T as Transport>::bind_unreliable(local_addr))
  }
}
