use std::time::Instant;

use crate::{
  showbiz::MessageHandoff,
  transport::Wire,
  types::{AckResponse, Message, NackResponse},
};
use futures::{Future, Stream};
use nodecraft::CheapClone;

use super::*;

impl<D, T> Showbiz<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  pub(crate) fn packet_listener(&self, shutdown_rx: async_channel::Receiver<()>) {
    let this = self.clone();
    let transport_rx = this.inner.transport.packet();
    <T::Runtime as Runtime>::spawn_detach(async move {
      loop {
        futures::select! {
          _ = shutdown_rx.recv().fuse() => {
            return;
          }
          packet = transport_rx.recv().fuse() => {
            match packet {
              Ok(packet) => {
                let (msg, addr, timestamp) = packet.into_components();
                match <T::Wire as Wire>::decode_message(&msg) {
                  Ok(msg) => {
                    this.handle_command(msg, addr, timestamp).await;
                  },
                  Err(e) => {
                    tracing::error!(target:  "showbiz.packet", "failed to decode packet: {}", e);
                  },
                }
              },
              Err(e) => {
                tracing::error!(target:  "showbiz.packet", "failed to receive packet: {}", e);
                // If we got an error, which means on the other side the transport has been closed,
                // so we need to return and shutdown the packet listener
                return;
              },
            }
          }
        }
      }
    });
  }

  #[async_recursion::async_recursion]
  async fn handle_command(
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
            tracing::warn!(target: "showbiz.packet", addr = %from, "handler queue full, dropping message ({})", msg.kind());
          } else {
            queue.push_back(MessageHandoff {
              msg,
              from: from.cheap_clone(),
            });
          }
        }

        // notify of pending message
        if let Err(e) = $this.inner.handoff_tx.send(()).await {
          tracing::error!(target: "showbiz.packet", addr = %from, err = %e, "failed to notify of pending message");
        }
      }};
    }

    match msg {
      Message::Compound(msgs) => {
        for msg in msgs {
          self
            .handle_command(msg, from.cheap_clone(), timestamp)
            .await;
        }
      }
      Message::Ping(ping) => self.handle_ping(ping, from).await,
      Message::IndirectPing(ind) => self.handle_indirect_ping(ind, from).await,
      Message::AckResponse(resp) => self.handle_ack(resp, timestamp).await,
      Message::NackResponse(resp) => self.handle_nack(resp).await,
      Message::Alive(alive) => {
        // Determine the message queue, prioritize alive
        {
          let mut mq = self.inner.queue.lock().await;
          let queue = &mut mq.high;

          // Check for overflow and append if not full
          if queue.len() >= self.inner.opts.handoff_queue_depth {
            tracing::warn!(target: "showbiz.packet", addr = %from, "handler queue full, dropping message (Alive)");
          } else {
            queue.push_back(MessageHandoff {
              msg: alive.into(),
              from: from.cheap_clone(),
            });
          }
        }

        // notify of pending message
        if let Err(e) = self.inner.handoff_tx.send(()).await {
          tracing::error!(target: "showbiz.packet", addr = %from, err = %e, "failed to notify of pending message");
        }
      }
      Message::Suspect(msg) => queue!(self.msg),
      Message::Dead(msg) => queue!(self.msg),
      Message::UserData(msg) => queue!(self.msg),
      mt => {
        tracing::error!(target: "showbiz.packet", addr = %from, err = "unexpected message type", message_type=mt.kind());
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
      tracing::error!(target:  "showbiz.packet", local=%self.inner.id, remote = %from, "got ping for unexpected node '{}'", p.target);
      return;
    }

    let msg = if let Some(delegate) = &self.delegate {
      let payload = match delegate.ack_payload().await {
        Ok(payload) => payload,
        Err(e) => {
          tracing::error!(target:  "showbiz.packet", local=%self.inner.id, remote = %from, err = %e, "failed to get ack payload from delegate");
          return;
        }
      };
      AckResponse {
        seq_no: p.seq_no,
        payload,
      }
    } else {
      AckResponse {
        seq_no: p.seq_no,
        payload: Bytes::new(),
      }
    };

    if let Err(e) = self.send_msg(p.source.address(), msg.into()).await {
      tracing::error!(target:  "showbiz.packet", addr = %from, err = %e, "failed to send ack response");
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
      .set_ack_handler(
        local_seq_no,
        self.inner.opts.probe_timeout,
        move |_payload, _timestamp| {
          async move {
            let _ = cancel_tx.send(());

            // Try to prevent the nack if we've caught it in time.
            let ack = AckResponse::new(ind_seq_no);
            if let Err(e) = this
              .send_msg(
                ind_source.address(),
                ack.into(),
              )
              .await
            {
              tracing::error!(target:  "showbiz.packet", addr = %afrom, err = %e, "failed to forward ack");
            }
          }
          .boxed()
        },
      )
      .await;

    if let Err(e) = self.send_msg(ind.target.address(), ping.into()).await {
      tracing::error!(target:  "showbiz.packet", addr = %from, err = %e, "failed to send ping");
    }

    // Setup a timer to fire off a nack if no ack is seen in time.
    let this = self.clone();
    let probe_timeout = self.inner.opts.probe_timeout;
    <T::Runtime as Runtime>::spawn_detach(async move {
      futures::select! {
        _ = <T::Runtime as Runtime>::sleep(probe_timeout).fuse() => {
          // We've not received an ack, so send a nack.
          let nack = NackResponse::new(ind.seq_no);
          if let Err(e) = this.send_msg(ind.source.address(), nack.into()).await {
            tracing::error!(target:  "showbiz.packet", local = %ind.source, remote = %from, err = %e, "failed to send nack");
          }
        }
        _ = cancel_rx.fuse() => {
          // We've received an ack, so we can cancel the nack.
        }
      }
    });
  }

  async fn handle_ack(&self, ack: AckResponse, timestamp: Instant) {
    self.invoke_ack_handler(ack, timestamp).await
  }

  async fn handle_nack(&self, nack: NackResponse) {
    self.invoke_nack_handler(nack).await
  }

  pub(crate) async fn send_msg(
    &self,
    addr: &<T::Resolver as AddressResolver>::ResolvedAddress,
    msg: Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    // Check if we can piggy back any messages
    let overhead = self.inner.transport.packet_overhead();
    let bytes_avail =
      self.inner.transport.packet_buffer_size() - <T::Wire as Wire>::encoded_len(&msg) - overhead;

    let mut msgs = self
      .get_broadcast_with_prepend(vec![msg], overhead, bytes_avail)
      .await?;

    // Fast path if nothing to piggypack
    if msgs.len() == 1 {
      return self.send_packet(addr, msgs.pop().unwrap()).await;
    }

    // Send the message
    self.send_packets(addr, msgs).await
  }
}
