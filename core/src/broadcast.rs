use bytes::Bytes;
use showbiz_traits::Broadcast;
use showbiz_types::SharedString;

pub(crate) struct ShowbizBroadcast {
  name: Option<SharedString>,
  node: String,
  msg: Bytes,
  #[cfg(feature = "async")]
  notify: async_channel::Sender<()>,
  #[cfg(not(feature = "async"))]
  notify: crossbeam_channel::Sender<()>,
}

impl Broadcast for ShowbizBroadcast {
  fn name(&self) -> Option<&str> {
    None
  }

  fn invalidates(&self, other: &Self) -> bool {
    todo!()
  }

  fn message(&self) -> &Bytes {
    todo!()
  }

  fn finished<'life0, 'async_trait>(
    &'life0 self,
  ) -> core::pin::Pin<Box<dyn core::future::Future<Output = ()> + core::marker::Send + 'async_trait>>
  where
    'life0: 'async_trait,
    Self: 'async_trait,
  {
    todo!()
  }

  fn is_unique(&self) -> bool {
    todo!()
  }
}
