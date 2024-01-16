use crate::{DelegateVersion, ProtocolVersion};

use super::*;
use nodecraft::Node;

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
#[cfg_attr(
  feature = "rkyv",
  archive_attr(derive(Debug, Clone, PartialEq, Eq, Hash))
)]
pub struct Alive<I, A> {
  incarnation: u32,
  meta: Bytes,
  node: Node<I, A>,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}
