//! Identity pass-through resolver — input is already a [`SocketAddr`].

use crate::resolver::Resolver;
use std::{io, net::SocketAddr};

/// Identity pass-through resolver — declares
/// [`Resolver::Address`](crate::resolver::Resolver::Address)`= SocketAddr`
/// and returns the input verbatim. Use when seeds are already concrete
/// socket addresses (no DNS, no hostname parsing).
pub struct SocketAddrResolver;

impl Resolver for SocketAddrResolver {
  type Address = SocketAddr;
  type Error = io::Error;

  async fn resolve(&self, addr: &Self::Address) -> Result<Vec<SocketAddr>, Self::Error> {
    Ok(vec![*addr])
  }
}
