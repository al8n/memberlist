use agnostic_lite::AsyncSpawner;
use futures::stream::{Stream, StreamExt};
use nodecraft::CheapClone;

use crate::{
  base::MessageHandoff,
  types::{Data, Message},
};

use super::*;

macro_rules! queue {
  ($this:ident.$msg:ident.$from:ident) => {{
    // Determine the message queue, prioritize alive
    {
      let mut mq = $this.inner.queue.lock().await;
      let queue = &mut mq.low;

      let msg: Message<_, _> = $msg.into();
      // Check for overflow and append if not full
      if queue.len() >= $this.inner.opts.handoff_queue_depth {
        tracing::warn!(addr = %$from, "memberlist.packet: handler queue full, dropping message ({})", msg.kind());
      } else {
        queue.push_back(MessageHandoff {
          msg,
          from: $from.cheap_clone(),
        });
      }
    }

    // notify of pending message
    if let Err(e) = $this.inner.handoff_tx.send(()).await {
      tracing::error!(addr = %$from, err = %e, "memberlist.packet: failed to notify of pending message");
    }
  }};
}

impl<D, T> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  pub(crate) fn packet_listener(
    &self,
    shutdown_rx: async_channel::Receiver<()>,
  ) -> <<T::Runtime as RuntimeLite>::Spawner as AsyncSpawner>::JoinHandle<()> {
    let this = self.clone();
    let packet_rx = this.inner.transport.packet();
    <T::Runtime as RuntimeLite>::spawn(async move {
      'outer: loop {
        futures::select! {
          _ = shutdown_rx.recv().fuse() => {
            break 'outer;
          }
          packet = packet_rx.recv().fuse() => {
            match packet {
              Ok(packet) => {
                let (addr, timestamp, packet) = packet.into_components();
                this.handle_messages(addr, timestamp, packet).await;
              },
              Err(e) => {
                if !this.inner.shutdown_tx.is_closed() {
                  tracing::error!("memberlist.packet: failed to receive packet: {}", e);
                }
                // If we got an error, which means on the other side the transport has been closed,
                // so we need to return and shutdown the packet listener
                break 'outer;
              },
            }
          }
        }
      }

      tracing::debug!("memberlist: packet listener exits");
    })
  }

  async fn handle_message(
    &self,
    from: &T::ResolvedAddress,
    timestamp: <T::Runtime as RuntimeLite>::Instant,
    msg: <Message<T::Id, T::ResolvedAddress> as Data>::Ref<'_>,
  ) {
    tracing::trace!(local = %self.advertise_address(), from = %from, packet=?msg, "memberlist.packet: handle packet");

    match msg {
      MessageRef::Ping(ping) => self.handle_ping(ping, from).await,
      MessageRef::IndirectPing(ind) => self.handle_indirect_ping(ind, from.cheap_clone()).await,
      MessageRef::Ack(resp) => match Ack::from_ref(resp) {
        Ok(ack) => self.handle_ack(ack, timestamp).await,
        Err(e) => {
          tracing::error!(addr = %from, err = %e, "memberlist.packet: failed to decode ack");
        }
      },
      MessageRef::Nack(resp) => self.handle_nack(resp).await,
      MessageRef::Alive(alive) => {
        // Determine the message queue, prioritize alive
        {
          let mut mq = self.inner.queue.lock().await;
          let queue = &mut mq.high;

          // Check for overflow and append if not full
          if queue.len() >= self.inner.opts.handoff_queue_depth {
            tracing::warn!(addr = %from, "memberlist.packet: handler queue full, dropping message (Alive)");
          } else {
            match Alive::<T::Id, T::ResolvedAddress>::from_ref(alive) {
              Ok(alive) => {
                queue.push_back(MessageHandoff {
                  msg: alive.into(),
                  from: from.cheap_clone(),
                });
              }
              Err(e) => {
                tracing::error!(addr = %from, err = %e, "memberlist.packet: failed to decode alive message");
                return;
              }
            }
          }
        }

        // notify of pending message
        if let Err(e) = self.inner.handoff_tx.send(()).await {
          tracing::error!(addr = %from, err = %e, "memberlist.packet: failed to notify of pending message");
        }
      }
      MessageRef::Suspect(msg) => {
        let msg = match Suspect::<T::Id>::from_ref(msg) {
          Ok(msg) => msg,
          Err(e) => {
            tracing::error!(addr = %from, err = %e, "memberlist.packet: failed to decode suspect message");
            return;
          }
        };
        queue!(self.msg.from)
      }
      MessageRef::Dead(msg) => {
        let msg = match Dead::<T::Id>::from_ref(msg) {
          Ok(msg) => msg,
          Err(e) => {
            tracing::error!(addr = %from, err = %e, "memberlist.packet: failed to decode dead message");
            return;
          }
        };
        queue!(self.msg.from)
      }
      MessageRef::UserData(msg) => {
        let msg = Bytes::copy_from_slice(msg);
        queue!(self.msg.from)
      }
      msg => {
        tracing::error!(addr = %from, err = "unexpected message type", message_type=msg.ty().kind(), "memberlist.packet");
      }
    }
  }

  async fn handle_messages(
    &self,
    from: T::ResolvedAddress,
    timestamp: <T::Runtime as RuntimeLite>::Instant,
    payload: Bytes,
  ) {
    let msg = match <MessageRef::<'_, <T::Id as Data>::Ref<'_>, <T::ResolvedAddress as Data>::Ref<'_>> as DataRef<Message<T::Id, T::ResolvedAddress>>>::decode(&payload) {
      Ok((_, msg)) => msg,
      Err(e) => {
        tracing::error!(addr = %from, err = %e, "memberlist.packet: failed to decode message");
        return;
      }
    };

    // match msg {
    //   MessageRef::Compound(decoder) => {
    //     for msg in decoder.iter::<T::Id, T::ResolvedAddress>() {
    //       match msg {
    //         Ok(msg) => {
    //           self.handle_message(&from, timestamp, msg).await;
    //         }
    //         Err(e) => {
    //           tracing::error!(addr = %from, err = %e, "memberlist.packet: failed to decode message");
    //         }
    //       }
    //     }
    //   }
    //   msg => self.handle_message(&from, timestamp, msg).await,
    // }
    todo!()
  }

  async fn handle_ping(
    &self,
    p: <Ping<T::Id, T::ResolvedAddress> as Data>::Ref<'_>,
    from: &T::ResolvedAddress,
  ) {
    match <T::Id as Data>::from_ref(*p.target().id()) {
      Ok(id) => {
        // If node is provided, verify that it is for us
        if id != self.inner.id {
          tracing::error!(local=%self.inner.id, remote = %from, "memberlist.packet: got ping for unexpected node '{:?}'", p.target());
          return;
        }
      }
      Err(e) => {
        tracing::error!(local=%self.inner.id, remote = %from, err = %e, "memberlist.packet: failed to decode target id");
        return;
      }
    }

    let msg = if let Some(delegate) = &self.delegate {
      Ack::new(p.sequence_number()).with_payload(delegate.ack_payload().await)
    } else {
      Ack::new(p.sequence_number())
    };

    let source_addr = match <T::ResolvedAddress as Data>::from_ref(*p.source().address()) {
      Ok(addr) => addr,
      Err(e) => {
        tracing::error!(local=%self.inner.id, remote = %from, err = %e, "memberlist.packet: failed to decode source address");
        return;
      }
    };

    if let Some(e) = Error::try_from_stream(self.send_msg(&source_addr, msg.into()).await).await {
      tracing::error!(addr = %from, err = %e, "memberlist.packet: failed to send ack response");
    }
  }

  async fn handle_indirect_ping(
    &self,
    ind: <IndirectPing<T::Id, T::ResolvedAddress> as Data>::Ref<'_>,
    from: T::ResolvedAddress,
  ) {
    // TODO: check protocol version and delegate version, currently we do not need to do this
    // because we only have one version

    // Send a ping to the correct host.
    let local_sequence_number = self.next_sequence_number();

    let ind = match IndirectPing::<T::Id, T::ResolvedAddress>::from_ref(ind) {
      Ok(target) => target,
      Err(e) => {
        tracing::error!(local=%self.inner.id, remote = %from, err = %e, "memberlist.packet: failed to decode indirect target");
        return;
      }
    };

    let ping = Ping::new(
      local_sequence_number,
      self.advertise_node(),
      ind.target().cheap_clone(),
    );

    // Forward the ack back to the requestor. If the request encodes an origin
    // use that otherwise assume that the other end of the UDP socket is
    // usable.

    let (cancel_tx, cancel_rx) = futures::channel::oneshot::channel::<()>();
    // Setup a response handler to relay the ack
    let this = self.clone();
    let ind_source = ind.source().cheap_clone();
    let ind_sequence_number = ind.sequence_number();
    let afrom = from.cheap_clone();

    self.inner.ack_manager.set_ack_handler::<_>(
      local_sequence_number,
      self.inner.opts.probe_timeout,
      move |_payload, _timestamp| {
        async move {
          let _ = cancel_tx.send(());

          // Try to prevent the nack if we've caught it in time.
          let ack = Ack::new(ind_sequence_number);
          if let Some(e) =
            Error::try_from_stream(this.send_msg(ind_source.address(), ack.into()).await).await
          {
            tracing::error!(addr = %afrom, err = %e, "memberlist.packet: failed to forward ack");
          }
        }
        .boxed()
      },
    );

    if let Some(e) =
      Error::try_from_stream(self.send_msg(ind.target().address(), ping.into()).await).await
    {
      tracing::error!(local = %self.local_id(), source = %ind.source(), target=%ind.target(), err = %e, "memberlist.packet: failed to send indirect ping");
    }

    // Setup a timer to fire off a nack if no ack is seen in time.
    let this = self.clone();
    let probe_timeout = self.inner.opts.probe_timeout;
    <T::Runtime as RuntimeLite>::spawn_detach(async move {
      futures::select! {
        _ = <T::Runtime as RuntimeLite>::sleep(probe_timeout).fuse() => {
          // We've not received an ack, so send a nack.
          let nack = Nack::new(ind.sequence_number());

          if let Some(e) = Error::try_from_stream(this.send_msg(ind.source().address(), nack.into()).await).await {
            tracing::error!(local = %ind.source(), remote = %from, err = %e, "memberlist.packet: failed to send nack");
          } else {
            tracing::trace!(local = %this.local_id(), source = %ind.source(), "memberlist.packet: send nack");
          }
        }
        res = cancel_rx.fuse() => {
          match res {
            Ok(_) => {
              // We've received an ack, so we can cancel the nack.
            }
            Err(_) => {
              // We've not received an ack, so send a nack.
              let nack = Nack::new(ind.sequence_number());

              if let Some(e) = Error::try_from_stream(this.send_msg(ind.source().address(), nack.into()).await).await {
                tracing::error!(local = %ind.source(), remote = %from, err = %e, "memberlist.packet: failed to send nack");
              } else {
                tracing::trace!(local = %this.local_id(), source = %ind.source(), "memberlist.packet: send nack");
              }
            }
          }
        }
      }
    });
  }

  async fn handle_ack(&self, ack: Ack, timestamp: <T::Runtime as RuntimeLite>::Instant) {
    self
      .inner
      .ack_manager
      .invoke_ack_handler(ack, timestamp)
      .await
  }

  async fn handle_nack(&self, nack: Nack) {
    self.inner.ack_manager.invoke_nack_handler(nack).await
  }

  #[auto_enums::auto_enum(futures03::Stream)]
  pub(crate) async fn send_msg<'a>(
    &'a self,
    addr: &'a T::ResolvedAddress,
    msg: Message<T::Id, T::ResolvedAddress>,
  ) -> impl Stream<Item = Error<T, D>> + Send + 'a {
    // Check if we can piggy back any messages
    let bytes_avail = self.inner.transport.max_packet_size()
      - msg.encoded_len()
      - self.inner.transport.packets_header_overhead();

    let msgs = self
      .get_broadcast_with_prepend(
        msg.into(),
        self.inner.transport.packet_overhead(),
        bytes_avail,
      )
      .await;

    match msgs {
      Err(e) => futures::stream::once(async { e }),
      Ok(msgs) => {
        // Send the message
        self
          .transport_send_packets(addr, &msgs)
          .filter_map(|res| async move {
            match res {
              Ok(_) => None,
              Err(e) => Some(e),
            }
          })
      }
    }
  }

  // pub(crate) async fn send_msg(
  //   &self,
  //   addr: &T::ResolvedAddress,
  //   msg: Message<T::Id, T::ResolvedAddress>,
  // ) -> Result<(), OneOrMore<Error<T, D>>> {
  //   // Check if we can piggy back any messages
  //   let bytes_avail = self.inner.transport.max_packet_size()
  //     - msg.encoded_len()
  //     - self.inner.transport.packets_header_overhead();

  //   let msgs = self
  //     .get_broadcast_with_prepend(
  //       msg.into(),
  //       self.inner.transport.packet_overhead(),
  //       bytes_avail,
  //     )
  //     .await
  //     .map_err(|e| OneOrMore::from(e))?;

  //   // Send the message
  //   let stream = self.transport_send_packets(addr, &msgs);
  //   futures::pin_mut!(stream);
  //   let errs = stream.filter_map(|res| async move {
  //     match res {
  //       Ok(_) => None,
  //       Err(e) => Some(e),
  //     }
  //   })
  //   .collect::<OneOrMore<_>>()
  //   .await;

  //   if errs.is_empty() {
  //     Ok(())
  //   } else {
  //     Err(errs)
  //   }
  // }
}
