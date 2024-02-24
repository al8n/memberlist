use std::time::Instant;

use crate::{
  base::MessageHandoff,
  transport::Wire,
  types::{Ack, Message, Nack},
};
use either::Either;
use futures::{Future, Stream};
use nodecraft::CheapClone;

use super::*;

impl<D, T> Memberlist<T, D>
where
  D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  pub(crate) fn packet_listener(&self, shutdown_rx: async_channel::Receiver<()>) -> <T::Runtime as Runtime>::JoinHandle<()> {
    let this = self.clone();
    let packet_rx = this.inner.transport.packet();
    <T::Runtime as Runtime>::spawn(async move {
      loop {
        futures::select! {
          _ = shutdown_rx.recv().fuse() => {
            return;
          }
          packet = packet_rx.recv().fuse() => {
            match packet {
              Ok(packet) => {
                let (msg, addr, timestamp) = packet.into_components();
                this.handle_messages(msg, addr, timestamp).await;
              },
              Err(e) => {
                if !this.inner.shutdown_tx.is_closed() {
                  tracing::error!(target =  "memberlist.packet", "failed to receive packet: {}", e);
                }
                // If we got an error, which means on the other side the transport has been closed,
                // so we need to return and shutdown the packet listener
                return;
              },
            }
          }
        }
      }
    })
  }

  async fn handle_message(
    &self,
    msg: Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    from: <T::Resolver as AddressResolver>::ResolvedAddress,
    timestamp: Instant,
  ) {
    macro_rules! queue {
      ($this:ident.$msg:ident) => {{
        // Determine the message queue, prioritize alive
        {
          let mut mq = $this.inner.queue.lock().await;
          let queue = &mut mq.low;

          let msg: Message<_, _> = $msg.into();
          // Check for overflow and append if not full
          if queue.len() >= $this.inner.opts.handoff_queue_depth {
            tracing::warn!(target = "memberlist.packet", addr = %from, "handler queue full, dropping message ({})", msg.kind());
          } else {
            queue.push_back(MessageHandoff {
              msg,
              from: from.cheap_clone(),
            });
          }
        }

        // notify of pending message
        if let Err(e) = $this.inner.handoff_tx.send(()).await {
          tracing::error!(target = "memberlist.packet", addr = %from, err = %e, "failed to notify of pending message");
        }
      }};
    }

    tracing::trace!(target = "memberlist.packet", local = %self.advertise_address(), from = %from, packet=?msg, "handle packet");

    match msg {
      Message::Ping(ping) => self.handle_ping(ping, from).await,
      Message::IndirectPing(ind) => self.handle_indirect_ping(ind, from).await,
      Message::Ack(resp) => self.handle_ack(resp, timestamp).await,
      Message::Nack(resp) => self.handle_nack(resp).await,
      Message::Alive(alive) => { 
        // Determine the message queue, prioritize alive
        {
          let mut mq = self.inner.queue.lock().await;
          let queue = &mut mq.high;

          // Check for overflow and append if not full
          if queue.len() >= self.inner.opts.handoff_queue_depth {
            tracing::warn!(target = "memberlist.packet", addr = %from, "handler queue full, dropping message (Alive)");
          } else {
            queue.push_back(MessageHandoff {
              msg: alive.into(),
              from: from.cheap_clone(),
            });
          }
        }

        // notify of pending message
        if let Err(e) = self.inner.handoff_tx.send(()).await {
          tracing::error!(target = "memberlist.packet", addr = %from, err = %e, "failed to notify of pending message");
        }
      }
      Message::Suspect(msg) => queue!(self.msg),
      Message::Dead(msg) => queue!(self.msg),
      Message::UserData(msg) => queue!(self.msg),
      mt => {
        tracing::error!(target = "memberlist.packet", addr = %from, err = "unexpected message type", message_type=mt.kind());
      }
    }
  }

  async fn handle_messages(
    &self,
    msgs: OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    from: <T::Resolver as AddressResolver>::ResolvedAddress,
    timestamp: Instant,
  ) {
    match msgs.into_either() {
      Either::Left(None) => {}
      Either::Left(Some(msg)) => self.handle_message(msg, from, timestamp).await,
      Either::Right(msgs) => {
        for msg in msgs {
          self
            .handle_message(msg, from.cheap_clone(), timestamp)
            .await
        }
      }
    }
  }

  async fn handle_ping(
    &self,
    p: Ping<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    from: <T::Resolver as AddressResolver>::ResolvedAddress,
  ) {
    // If node is provided, verify that it is for us
    if p.target.id().ne(&self.inner.id) {
      tracing::error!(target =  "memberlist.packet", local=%self.inner.id, remote = %from, "got ping for unexpected node '{}'", p.target);
      return;
    }

    let msg = if let Some(delegate) = &self.delegate {
      Ack {
        seq_no: p.seq_no,
        payload: delegate.ack_payload().await,
      }
    } else {
      Ack {
        seq_no: p.seq_no,
        payload: Bytes::new(),
      }
    };
    if let Err(e) = self.send_msg(p.source.address(), msg.into()).await {
      tracing::error!(target =  "memberlist.packet", addr = %from, err = %e, "failed to send ack response");
    }
  }

  async fn handle_indirect_ping(
    &self,
    ind: IndirectPing<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    from: <T::Resolver as AddressResolver>::ResolvedAddress,
  ) {
    // TODO: check protocol version and delegate version, currently we do not need to do this
    // because we only have one version

    // Send a ping to the correct host.
    tracing::error!("DEBUG: call next seq no from handle indirect ping");
    let local_seq_no = self.next_seq_no();

    let ping = Ping {
      seq_no: local_seq_no,
      source: self.advertise_node(),
      target: ind.target.cheap_clone(),
    };

    // Forward the ack back to the requestor. If the request encodes an origin
    // use that otherwise assume that the other end of the UDP socket is
    // usable.

    let (cancel_tx, cancel_rx) = futures::channel::oneshot::channel::<()>();
    // Setup a response handler to relay the ack
    let this = self.clone();
    let ind_source = ind.source.cheap_clone();
    let ind_seq_no = ind.seq_no;
    let afrom = from.cheap_clone();

    self
      .inner
      .ack_manager
      .set_ack_handler::<_, T::Runtime>(
        local_seq_no,
        self.inner.opts.probe_timeout,
        move |_payload, _timestamp| {
          async move {
            let _ = cancel_tx.send(());

            // Try to prevent the nack if we've caught it in time.
            let ack = Ack::new(ind_seq_no);
            if let Err(e) = this
              .send_msg(
                ind_source.address(),
                ack.into(),
              )
              .await
            {
              tracing::error!(target =  "memberlist.packet", addr = %afrom, err = %e, "failed to forward ack");
            }
          }
          .boxed()
        },
      );

    if let Err(e) = self.send_msg(ind.target.address(), ping.into()).await {
      tracing::error!(target =  "memberlist.packet", addr = %from, err = %e, "failed to send ping");
    }

    // Setup a timer to fire off a nack if no ack is seen in time.
    let this = self.clone();
    let probe_timeout = self.inner.opts.probe_timeout;
    <T::Runtime as Runtime>::spawn_detach(async move {
      futures::select! {
        _ = <T::Runtime as Runtime>::sleep(probe_timeout).fuse() => {
          // We've not received an ack, so send a nack.
          let nack = Nack::new(ind.seq_no);
          if let Err(e) = this.send_msg(ind.source.address(), nack.into()).await {
            tracing::error!(target =  "memberlist.packet", local = %ind.source, remote = %from, err = %e, "failed to send nack");
          }
        }
        _ = cancel_rx.fuse() => {
          // We've received an ack, so we can cancel the nack.
        }
      }
    });
  }

  async fn handle_ack(&self, ack: Ack, timestamp: Instant) {
    self
      .inner
      .ack_manager
      .invoke_ack_handler(ack, timestamp)
      .await
  }

  async fn handle_nack(&self, nack: Nack) {
    self.inner.ack_manager.invoke_nack_handler(nack).await
  }

  pub(crate) async fn send_msg(
    &self,
    addr: &<T::Resolver as AddressResolver>::ResolvedAddress,
    msg: Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    // Check if we can piggy back any messages
    let bytes_avail = self.inner.transport.max_payload_size()
      - <T::Wire as Wire>::encoded_len(&msg)
      - self.inner.transport.packets_header_overhead();

    let mut msgs = self
      .get_broadcast_with_prepend(
        msg.into(),
        self.inner.transport.packet_overhead(),
        bytes_avail,
      )
      .await?;
    // Fast path if nothing to piggypack
    if msgs.len() == 1 {
      return self.transport_send_packet(addr, msgs.pop().unwrap()).await;
    }

    // Send the message
    self.transport_send_packets(addr, msgs).await
  }
}
