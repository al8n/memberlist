use std::{
  collections::HashSet,
  sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use super::{delegate::Delegate, transport::Transport, types::Epoch, *};
use agnostic_lite::{AfterHandle, AsyncAfterSpawner, RuntimeLite};
use memberlist_types::{Dead, State};
use nodecraft::resolver::AddressResolver;

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

pub(crate) struct Suspicioner<T, D>
where
  T: Transport,
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  memberlist: Memberlist<T, D>,
  node: T::Id,
  change_time: Epoch,
  incarnation: u32,
  #[cfg(feature = "metrics")]
  k: isize,
}

impl<T, D> Suspicioner<T, D>
where
  T: Transport,
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  pub(crate) fn new(
    memberlist: Memberlist<T, D>,
    node: T::Id,
    change_time: Epoch,
    incarnation: u32,
    #[cfg(feature = "metrics")] k: isize,
  ) -> Self {
    Suspicioner {
      memberlist,
      node,
      change_time,
      k,
      incarnation,
    }
  }

  async fn suspicion(&self, num_confirmations: u32) {
    let Self {
      memberlist: t,
      node: n,
      change_time,
      incarnation,
      #[cfg(feature = "metrics")]
      k,
    } = self;
    let timeout = {
      let members = t.inner.nodes.read().await;

      members.node_map.get(n).and_then(|&idx| {
        let state = &members.nodes[idx];
        let timeout =
          state.state.state == State::Suspect && state.state.state_change == *change_time;
        if timeout {
          Some(Dead::new(
            *incarnation,
            state.id().cheap_clone(),
            t.local_id().cheap_clone(),
          ))
        } else {
          None
        }
      })
    };

    if let Some(dead) = timeout {
      #[cfg(feature = "metrics")]
      {
        if *k > 0 && *k > num_confirmations as isize {
          metrics::counter!(
            "memberlist.degraded.timeout",
            t.inner.opts.metric_labels.iter()
          )
          .increment(1);
        }
      }

      tracing::info!(
        "memberlist.state: marking {} as failed, suspect timeout reached ({} peer confirmations)",
        dead.node(),
        num_confirmations
      );
      let mut memberlist = t.inner.nodes.write().await;
      let dead_node = dead.node().cheap_clone();
      if let Err(e) = t.dead_node(&mut memberlist, dead).await {
        tracing::error!(err=%e, "memberlist.state: failed to mark {dead_node} as failed");
      }
    }
  }
}

pub(crate) struct Suspicion<T, D>
where
  T: Transport,
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  n: Arc<AtomicU32>,
  k: u32,
  min: Duration,
  max: Duration,
  start: Instant,
  handle: Option<<<T::Runtime as RuntimeLite>::AfterSpawner as AsyncAfterSpawner>::JoinHandle<()>>,
  suspicioner: Arc<Suspicioner<T, D>>,
  confirmations: HashSet<T::Id>,
}

impl<T, D> Suspicion<T, D>
where
  T: Transport,
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  /// Returns a after_func started with the max time, and that will drive
  /// to the min time after seeing k or more confirmations. The from node will be
  /// excluded from confirmations since we might get our own suspicion message
  /// gossiped back to us. The minimum time will be used if no confirmations are
  /// called for (k = 0).
  pub(crate) fn new(
    from: T::Id,
    k: isize,
    min: Duration,
    max: Duration,
    suspicioner: Suspicioner<T, D>,
  ) -> Self {
    #[allow(clippy::mutable_key_type)]
    let confirmations = [from].into_iter().collect();
    let n = Arc::new(AtomicU32::new(0));
    let timeout = if (k as u32) < 1 { min } else { max };
    let suspicioner = Arc::new(suspicioner);
    let n1 = n.clone();
    let s1 = suspicioner.clone();
    let handle = <T::Runtime as RuntimeLite>::spawn_after(timeout, async move {
      s1.suspicion(n1.load(Ordering::SeqCst)).await;
    });

    Suspicion {
      n,
      k: k as u32,
      min,
      max,
      start: Instant::now(),
      suspicioner,
      handle: Some(handle),
      confirmations,
    }
  }
}

impl<T, D> Suspicion<T, D>
where
  T: Transport,
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  /// Confirm registers that a possibly new peer has also determined the given
  /// node is suspect. This returns true if this was new information, and false
  /// if it was a duplicate confirmation, or if we've got enough confirmations to
  /// hit the minimum.
  pub(crate) async fn confirm(&mut self, from: &T::Id) -> bool {
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
          let suspicioner = self.suspicioner.clone();
          self.handle = Some(<T::Runtime as RuntimeLite>::spawn_after_at(
            Instant::now() + remaining,
            async move {
              suspicioner.suspicion(n.load(Ordering::SeqCst)).await;
            },
          ));
        } else {
          let n = self.n.clone();
          let suspicioner = self.suspicioner.clone();
          <T::Runtime as RuntimeLite>::spawn_detach(async move {
            suspicioner.suspicion(n.load(Ordering::SeqCst)).await;
          });
        }
      }
    }

    true
  }
}

impl<T, D> Drop for Suspicion<T, D>
where
  T: Transport,
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  fn drop(&mut self) {
    if let Some(h) = self.handle.take() {
      h.abort();
    }
  }
}

#[cfg(test)]
mod tests {
  //   use std::net::SocketAddr;

  //   use async_channel::TryRecvError;
  //   use memberlist_types::{Message, TinyVec};
  //   use nodecraft::resolver::socket_addr::SocketAddrResolver;
  // use smol_str::SmolStr;

  //   use self::{
  //     delegate::VoidDelegate, error::TransportError, transport::{PacketSubscriber, StreamSubscriber, TimeoutableReadStream, TimeoutableWriteStream}
  //   };

  use super::*;

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
  // struct Pair {
  //   from: &'static str,
  //   new_info: bool,
  // }

  // const K: u32 = 3;
  // const MIN: Duration = Duration::from_millis(500);
  // const MAX: Duration = Duration::from_secs(2);

  // fn test_cases() -> Vec<(i32, &'static str, Vec<Pair>, Duration)> {
  //   vec![
  //     (0, "me", vec![], MAX),
  //     (
  //       1,
  //       "me",
  //       vec![
  //         Pair {
  //           from: "me",
  //           new_info: false,
  //         },
  //         Pair {
  //           from: "foo",
  //           new_info: true,
  //         },
  //       ],
  //       Duration::from_millis(1250),
  //     ),
  //     (
  //       1,
  //       "me",
  //       vec![
  //         Pair {
  //           from: "me",
  //           new_info: false,
  //         },
  //         Pair {
  //           from: "foo",
  //           new_info: true,
  //         },
  //         Pair {
  //           from: "foo",
  //           new_info: false,
  //         },
  //         Pair {
  //           from: "foo",
  //           new_info: false,
  //         },
  //       ],
  //       Duration::from_millis(1250),
  //     ),
  //     (
  //       2,
  //       "me",
  //       vec![
  //         Pair {
  //           from: "me",
  //           new_info: false,
  //         },
  //         Pair {
  //           from: "foo",
  //           new_info: true,
  //         },
  //         Pair {
  //           from: "bar",
  //           new_info: true,
  //         },
  //       ],
  //       Duration::from_millis(810),
  //     ),
  //     (
  //       3,
  //       "me",
  //       vec![
  //         Pair {
  //           from: "me",
  //           new_info: false,
  //         },
  //         Pair {
  //           from: "foo",
  //           new_info: true,
  //         },
  //         Pair {
  //           from: "bar",
  //           new_info: true,
  //         },
  //         Pair {
  //           from: "baz",
  //           new_info: true,
  //         },
  //       ],
  //       MIN,
  //     ),
  //     (
  //       3,
  //       "me",
  //       vec![
  //         Pair {
  //           from: "me",
  //           new_info: false,
  //         },
  //         Pair {
  //           from: "foo",
  //           new_info: true,
  //         },
  //         Pair {
  //           from: "bar",
  //           new_info: true,
  //         },
  //         Pair {
  //           from: "baz",
  //           new_info: true,
  //         },
  //         Pair {
  //           from: "zoo",
  //           new_info: false,
  //         },
  //       ],
  //       MIN,
  //     ),
  //   ]
  // }

  // struct DummyWire;

  // impl crate::transport::Wire for DummyWire {
  //   type Error = std::io::Error;
  //   type Address = SocketAddr;
  //   type Id = SmolStr;

  //   fn encoded_len(_msg: &Message<Self::Id, Self::Address>) -> usize {
  //     unreachable!()
  //   }

  //   fn encode_message(
  //     _msg: Message<Self::Id, Self::Address>,
  //     _dst: &mut [u8],
  //   ) -> Result<usize, Self::Error> {
  //     unreachable!()
  //   }

  //   fn decode_message(
  //     _src: &[u8],
  //   ) -> Result<(usize, Message<Self::Id, Self::Address>), Self::Error> {
  //     unreachable!()
  //   }

  //   async fn decode_message_from_reader(
  //     _conn: impl futures::prelude::AsyncRead + Send + Unpin,
  //   ) -> std::io::Result<(usize, Message<Self::Id, Self::Address>)> {
  //     unreachable!()
  //   }
  // }

  // #[derive(thiserror::Error, Debug)]
  // #[error("dummy transport error")]
  // struct DummyTransportError;

  // impl TransportError for DummyTransportError {
  //   fn is_remote_failure(&self) -> bool {
  //     unreachable!()
  //   }

  //   fn custom(_err: std::borrow::Cow<'static, str>) -> Self {
  //     unreachable!()
  //   }
  // }

  // struct DummyStream;

  // impl TimeoutableWriteStream for DummyStream {
  //   fn set_write_deadline(&mut self, _deadline:Option<Instant>) {
  //     unreachable!()
  //   }

  //   fn write_deadline(&self) -> Option<Instant>{
  //     unreachable!()
  //   }
  // }

  // impl TimeoutableReadStream for DummyStream {
  //   fn set_read_deadline(&mut self, _deadline:Option<Instant>) {
  //     unreachable!()
  //   }

  //   fn read_deadline(&self) -> Option<Instant>{
  //     unreachable!()
  //   }
  // }

  // type DummyDelegate = VoidDelegate<SmolStr, SocketAddr>;

  // struct DummyTransport;

  // impl Transport for DummyTransport {
  //   /// The error type for the transport
  //   type Error = DummyTransportError;
  //   /// The id type used to identify nodes
  //   type Id = SmolStr;
  //   /// The address resolver used to resolve addresses
  //   type Resolver = SocketAddrResolver<agnostic_lite::tokio::TokioRuntime>;
  //   /// The promised stream used to send and receive messages
  //   type Stream = DummyStream;
  //   /// The wire used to encode and decode messages
  //   type Wire = DummyWire;
  //   /// The async runtime
  //   type Runtime = agnostic_lite::tokio::TokioRuntime;

  //   /// Resolves the given address to a resolved address
  //   async fn resolve(
  //     &self,
  //     _addr: &<Self::Resolver as AddressResolver>::Address,
  //   ) -> Result<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Error> {
  //     unreachable!()
  //   }

  //   /// Returns the local id of the node
  //   fn local_id(&self) -> &Self::Id {
  //     unreachable!()
  //   }

  //   /// Returns the local address of the node
  //   fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address {
  //     unreachable!()
  //   }

  //   /// Returns the advertise address of the node
  //   fn advertise_address(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
  //     unreachable!()
  //   }

  //   /// Returns the maximum size of a packet that can be sent
  //   fn max_payload_size(&self) -> usize {
  //     unreachable!()
  //   }

  //   /// Returns the size of header overhead when trying to send messages through packet stream ([`Transport::send_packets`]).
  //   ///
  //   /// e.g. if every time invoking [`Transport::send_packets`],
  //   /// the concrete implementation wants to  add a header of 10 bytes,
  //   /// then the packet overhead is 10 bytes.
  //   fn packets_header_overhead(&self) -> usize {
  //     unreachable!()
  //   }

  //   /// Returns the size of overhead for per [`Message`] when trying to send messages through packet stream ([`Transport::send_packets`]).
  //   fn packet_overhead(&self) -> usize {
  //     unreachable!()
  //   }

  //   /// Returns an error if the given address is blocked
  //   fn blocked_address(
  //     &self,
  //     addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  //   ) -> Result<(), Self::Error> {
  //     unreachable!()
  //   }

  //   /// Reads a message from the remote node by promised connection.
  //   ///
  //   /// Returns the number of bytes read and the message.
  //   async fn read_message(
  //     &self,
  //     _from: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  //     _conn: &mut Self::Stream,
  //   ) -> Result<
  //     (
  //       usize,
  //       Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  //     ),
  //     Self::Error,
  //   > {
  //     unreachable!()
  //   }

  //   /// Sends a message to the remote node by promised connection.
  //   ///
  //   /// Returns the number of bytes sent.
  //   async fn send_message(
  //     &self,
  //     _conn: &mut Self::Stream,
  //     _msg: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  //   ) -> Result<usize, Self::Error> {
  //     unreachable!()
  //   }

  //   /// A packet-oriented interface that fires off the given
  //   /// payload to the given address in a connectionless fashion.
  //   ///
  //   /// # Returns
  //   ///
  //   /// - number of bytes sent
  //   /// - a time stamp that's as close as possible to when the packet
  //   /// was transmitted to help make accurate RTT measurements during probes.
  //   async fn send_packet(
  //     &self,
  //     _addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  //     _packet: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  //   ) -> Result<(usize, Instant), Self::Error> {
  //     unreachable!()
  //   }

  //   /// A packet-oriented interface that fires off the given
  //   /// payload to the given address in a connectionless fashion.
  //   ///
  //   /// # Returns
  //   ///
  //   /// - number of bytes sent
  //   /// - a time stamp that's as close as possible to when the packet
  //   /// was transmitted to help make accurate RTT measurements during probes.
  //   async fn send_packets(
  //     &self,
  //     _addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  //     _packets: TinyVec<Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>>,
  //   ) -> Result<(usize, Instant), Self::Error> {
  //     unreachable!()
  //   }

  //   /// Used to create a connection that allows us to perform
  //   /// two-way communication with a peer. This is generally more expensive
  //   /// than packet connections so is used for more infrequent operations
  //   /// such as anti-entropy or fallback probes if the packet-oriented probe
  //   /// failed.
  //   async fn dial_with_deadline(
  //     &self,
  //     _addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  //     _deadline: Instant,
  //   ) -> Result<Self::Stream, Self::Error> {
  //     unreachable!()
  //   }

  //   /// Used to cache a connection for future use.
  //   async fn cache_stream(
  //     &self,
  //     _addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  //     _stream: Self::Stream,
  //   ) -> Result<(), Self::Error> {
  //     unreachable!()
  //   }

  //   /// Returns a packet subscriber that can be used to receive incoming packets
  //   fn packet(
  //     &self,
  //   ) -> PacketSubscriber<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress> {
  //     unreachable!()
  //   }

  //   /// Returns a receiver that can be read to handle incoming stream
  //   /// connections from other peers. How this is set up for listening is
  //   /// left as an exercise for the concrete transport implementations.
  //   fn stream(
  //     &self,
  //   ) -> StreamSubscriber<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Stream> {
  //     unreachable!()
  //   }

  //   /// Shutdown the transport
  //   async fn shutdown(&self) -> Result<(), Self::Error> {
  //     unreachable!()
  //   }
  // }

  // #[tokio::test]
  // async fn test_suspicion_after_func() {
  //   use futures::FutureExt;

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

  //     let mut s = Suspicion::<DummyTransport, VoidDelegate<>>::new(from.into(), K, MIN, MAX, f);
  //     let fudge = Duration::from_millis(25);
  //     for p in confirmations.iter() {
  //       tokio::time::sleep(fudge).await;
  //       assert_eq!(
  //         s.confirm(&p.from.into()).await,
  //         p.new_info,
  //         "case {}: newInfo mismatch for {}",
  //         i,
  //         p.from
  //       );
  //     }

  //     let sleep = expected - fudge * (confirmations.len() as u32) - fudge;
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
  //     s.confirm(&"late".into()).await;
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
  //   use futures::FutureExt;

  //   let (tx, rx) = async_channel::unbounded();
  //   let f = move |_| {
  //     let tx = tx.clone();
  //     async move {
  //       tx.send(()).await.unwrap();
  //     }
  //     .boxed()
  //   };

  //   let mut s = Suspicion::<DummyTransport, DummyDelegate>::new(
  //     "me".into(),
  //     0,
  //     Duration::from_millis(25),
  //     Duration::from_secs(30),
  //     f,
  //   );

  //   assert!(!s.confirm(&"foo".into()).await);
  //   let _ = tokio::time::timeout(Duration::from_millis(50), rx.recv())
  //     .await
  //     .unwrap();
  // }

  // #[tokio::test]
  // async fn test_suspicion_after_func_immediate() {
  //   use futures::FutureExt;

  //   let (tx, rx) = async_channel::unbounded();
  //   let f = move |_| {
  //     let tx = tx.clone();
  //     async move {
  //       tx.send(()).await.unwrap();
  //     }
  //     .boxed()
  //   };

  //   let mut s = Suspicion::<DummyTransport, DummyDelegate>::new(
  //     "me".into(),
  //     1,
  //     Duration::from_millis(100),
  //     Duration::from_secs(30),
  //     f,
  //   );

  //   tokio::time::sleep(Duration::from_millis(200)).await;
  //   s.confirm(&"foo".into()).await;

  //   let _ = tokio::time::timeout(Duration::from_millis(25), rx.recv())
  //     .await
  //     .unwrap();
  // }
}
