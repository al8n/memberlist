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

/// Unit tests for create a `Memberlist`.
pub async fn test_memberlist_create() {}

/// Unit tests for create a `Memberlist` and shutdown.
pub async fn test_memberlist_create_shutdown() {}

/// Unit tests for the members of a `Memberlist`.
pub async fn test_memberlist_members() {}

/// Unit tests for join a `Memberlist`.
pub async fn test_memberlist_join() {}

/// Unit tests for join a `Memberlist` and cancel.
pub async fn test_memberlist_join_cancel() {}

/// Unit tests for join a `Memberlist` and cancel passive.
pub async fn test_memberlist_join_cancel_passive() {}

/// Unit tests for join and shutdown a `Memberlist`.
pub async fn test_memberlist_join_shutdown() {}

/// Unit test for join a dead node
pub async fn test_memberlist_join_dead_node() {}

/// Unit test for node delegate meta
pub async fn test_memberlist_node_delegate_meta() {}

/// Unit test for node delegate meta update
pub async fn test_memberlist_node_delegate_meta_update() {}

/// Unit test for user data
pub async fn test_memberlist_user_data() {}

/// Unit test for send
pub async fn test_memberlist_send() {}

/// Unit tests for leave
pub async fn test_memberlist_leave() {}

/// Unit test for advertise addr
pub async fn test_memberlist_advertise_addr() {}

/// Unit test for conflict delegate
pub async fn test_memberlist_conflict_delegate() {}

/// Unit test for ping delegate
pub async fn test_memberlist_ping_delegate() {}
