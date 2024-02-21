use super::*;

impl<I: Eq + core::hash::Hash, A, R> Members<I, A, R> {
  pub(crate) fn get_state<Q>(&self, id: &Q) -> Option<LocalNodeState<I, A>>
  where
    I: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq,
  {
    self
      .node_map
      .get(id)
      .map(|idx| self.nodes[*idx].state.clone())
  }

  pub(crate) fn set_state<Q>(&mut self, id: &Q, new_state: crate::types::State)
  where
    I: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq,
  {
    if let Some(idx) = self.node_map.get(id) {
      let state = &mut self.nodes[*idx].state;
      state.state = new_state;
    }
  }
}

impl<D, T> Memberlist<T, D>
where
  D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  #[cfg(any(test, feature = "test"))]
  pub(crate) async fn change_node<F>(&self, id: &T::Id, f: F)
  where
    F: Fn(&mut LocalNodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>),
  {
    let mut nodes = self.inner.nodes.write().await;
    if let Some(n) = nodes.node_map.get(id).copied() {
      f(&mut nodes.nodes[n].state);
    }
  }

  #[cfg(any(test, feature = "test"))]
  pub(crate) async fn get_node_state(&self, id: &T::Id) -> Option<crate::types::State> {
    let nodes = self.inner.nodes.read().await;
    nodes.node_map.get(id).map(|n| nodes.nodes[*n].state.state)
  }

  #[cfg(any(test, feature = "test"))]
  pub(crate) async fn get_node_state_change(&self, id: &T::Id) -> Option<std::time::Instant> {
    let nodes = self.inner.nodes.read().await;
    nodes.node_map.get(id).map(|n| nodes.nodes[*n].state_change)
  }
}
