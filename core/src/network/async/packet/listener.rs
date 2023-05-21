use super::*;

impl<T, D> Showbiz<T, D>
where
  T: Transport,
  D: Delegate,
{
  pub(crate) fn packet_listener<R, S>(&self, spawner: S)
  where
    R: Send + Sync + 'static,
    S: Fn(BoxFuture<'static, ()>) -> R + Copy + Send + Sync + 'static,
  {
    let this = self.clone();
    (spawner)(Box::pin(async move {
      loop {
        futures_util::select! {
          _ = this.inner.shutdown_rx.recv().fuse() => {
            return;
          }
          packet = this.inner.transport.packet().recv().fuse() => {
            match packet {
              Ok(packet) => this.ingest_packet(packet).await,
              Err(e) => tracing::error!(target = "showbiz", "failed to receive packet: {}", e),
            }
          }
        }
      }
    }));
  }

  async fn ingest_packet(&self, packet: Packet) {}
}
