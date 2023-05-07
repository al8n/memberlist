use bytes::Bytes;
use showbiz_traits::Broadcast;
use showbiz_types::SmolStr;

pub(crate) struct ShowbizBroadcast {
  name: Option<SmolStr>,
  node: SmolStr,
  msg: Bytes,
  #[cfg(feature = "async")]
  notify: async_channel::Sender<()>,
  #[cfg(not(feature = "async"))]
  notify: crossbeam_channel::Sender<()>,
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl Broadcast for ShowbizBroadcast {
  fn name(&self) -> Option<&SmolStr> {
    self.name.as_ref()
  }

  fn invalidates(&self, _other: &Self) -> bool {
    todo!()
  }

  fn message(&self) -> &Bytes {
    todo!()
  }

  #[cfg(feature = "async")]
  async fn finished(&self) {
    todo!()
  }

  #[cfg(not(feature = "async"))]
  fn finished(&self) {
    todo!()
  }

  fn is_unique(&self) -> bool {
    todo!()
  }
}
