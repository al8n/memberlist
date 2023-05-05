mod net;
pub use net::*;

pub(crate) use sealed::*;

#[cfg(feature = "sync")]
mod sealed {
  pub(crate) use crossbeam_channel::{
    unbounded, Receiver as UnboundedReceiver, Receiver, Sender as UnboundedSender, Sender,
  };
  pub(crate) use std::{
    io::Error as IOError,
    net::{TcpListener, TcpStream, UdpSocket},
    thread::{sleep, spawn},
  };
  pub(crate) use wg::WaitGroup;
}

#[cfg(feature = "tokio-showbiz")]
mod sealed {
  pub(crate) use async_channel::{
    unbounded, Receiver as UnboundedReceiver, Receiver, Sender as UnboundedSender, Sender,
  };
  pub(crate) use tokio::{
    io::Error as IOError,
    net::{TcpListener, TcpStream, UdpSocket},
    spawn,
    time::sleep,
  };
  pub(crate) use wg::AsyncWaitGroup as WaitGroup;
}

#[cfg(feature = "smol-showbiz")]
mod sealed {
  pub(crate) use async_channel::{
    unbounded, Receiver as UnboundedReceiver, Receiver, Sender as UnboundedSender, Sender,
  };
  pub(crate) use smol::{
    io::Error as IOError,
    net::{TcpListener, TcpStream, UdpSocket},
    spawn as smol_spawn,
  };
  pub(crate) use wg::AsyncWaitGroup as WaitGroup;

  pub(crate) async fn sleep(duration: std::time::Duration) {
    use smol::Timer;
    Timer::after(duration).await;
  }

  pub(crate) fn spawn<T: Send + 'static>(
    future: impl core::future::Future<Output = T> + Send + 'static,
  ) {
    smol_spawn(future).detach();
  }
}

#[cfg(feature = "async-showbiz")]
mod sealed {
  pub(crate) use async_channel::{
    unbounded, Receiver as UnboundedReceiver, Receiver, Sender as UnboundedSender, Sender,
  };
  pub(crate) use async_std::{
    io::Error as IOError,
    net::{TcpListener, TcpStream, UdpSocket},
    task::{sleep, spawn},
  };
  pub(crate) use wg::AsyncWaitGroup as WaitGroup;
}
