use super::*;

use bytes::Bytes;

/// Tag constants for the owned `Message` enum variants.
pub mod message_tags {
  /// Ping message tag
  pub const PING: u8 = 2;
  /// IndirectPing message tag
  pub const INDIRECT_PING: u8 = 3;
  /// Ack message tag
  pub const ACK: u8 = 4;
  /// Suspect message tag
  pub const SUSPECT: u8 = 5;
  /// Alive message tag
  pub const ALIVE: u8 = 6;
  /// Dead message tag
  pub const DEAD: u8 = 7;
  /// PushPull message tag
  pub const PUSH_PULL: u8 = 8;
  /// UserData message tag
  pub const USER_DATA: u8 = 9;
  /// Nack message tag
  pub const NACK: u8 = 10;
  /// ErrorResponse message tag
  pub const ERROR_RESPONSE: u8 = 11;
}

/// Owned message enum — the application-facing representation of all
/// memberlist gossip messages. Carries NO wire codec. [`crate::bridge`]
/// bridges this to the buffa-generated concrete codec via [`crate::data::Data`].
#[derive(
  Debug,
  Clone,
  derive_more::From,
  derive_more::IsVariant,
  derive_more::Unwrap,
  derive_more::TryUnwrap,
  PartialEq,
  Eq,
  Hash,
)]
#[non_exhaustive]
pub enum Message<I, A> {
  /// Ping message
  Ping(Ping<I, A>),
  /// Indirect ping message
  IndirectPing(IndirectPing<I, A>),
  /// Ack response message
  Ack(Ack),
  /// Suspect message
  Suspect(Suspect<I>),
  /// Alive message
  Alive(Alive<I, A>),
  /// Dead message
  Dead(Dead<I>),
  /// PushPull message
  PushPull(PushPull<I, A>),
  /// User mesg, not handled by us
  UserData(Bytes),
  /// Nack response message
  Nack(Nack),
  /// Error response message
  ErrorResponse(ErrorResponse),
}

impl<I, A> Message<I, A> {
  /// Returns the wire tag byte for this message variant.
  #[inline(always)]
  pub const fn tag(&self) -> u8 {
    match self {
      Self::Ping(_) => message_tags::PING,
      Self::IndirectPing(_) => message_tags::INDIRECT_PING,
      Self::Ack(_) => message_tags::ACK,
      Self::Suspect(_) => message_tags::SUSPECT,
      Self::Alive(_) => message_tags::ALIVE,
      Self::Dead(_) => message_tags::DEAD,
      Self::PushPull(_) => message_tags::PUSH_PULL,
      Self::UserData(_) => message_tags::USER_DATA,
      Self::Nack(_) => message_tags::NACK,
      Self::ErrorResponse(_) => message_tags::ERROR_RESPONSE,
    }
  }

  /// Construct a [`Message`] from a [`Ping`].
  #[inline(always)]
  pub const fn ping(val: Ping<I, A>) -> Self {
    Self::Ping(val)
  }

  /// Construct a [`Message`] from an [`IndirectPing`].
  #[inline(always)]
  pub const fn indirect_ping(val: IndirectPing<I, A>) -> Self {
    Self::IndirectPing(val)
  }

  /// Construct a [`Message`] from an [`Ack`].
  #[inline(always)]
  pub const fn ack(val: Ack) -> Self {
    Self::Ack(val)
  }

  /// Construct a [`Message`] from a [`Suspect`].
  #[inline(always)]
  pub const fn suspect(val: Suspect<I>) -> Self {
    Self::Suspect(val)
  }

  /// Construct a [`Message`] from an [`Alive`].
  #[inline(always)]
  pub const fn alive(val: Alive<I, A>) -> Self {
    Self::Alive(val)
  }

  /// Construct a [`Message`] from a [`Dead`].
  #[inline(always)]
  pub const fn dead(val: Dead<I>) -> Self {
    Self::Dead(val)
  }

  /// Construct a [`Message`] from a [`PushPull`].
  #[inline(always)]
  pub const fn push_pull(val: PushPull<I, A>) -> Self {
    Self::PushPull(val)
  }

  /// Construct a [`Message`] from user data bytes.
  #[inline(always)]
  pub const fn user_data(val: Bytes) -> Self {
    Self::UserData(val)
  }

  /// Construct a [`Message`] from a [`Nack`].
  #[inline(always)]
  pub const fn nack(val: Nack) -> Self {
    Self::Nack(val)
  }

  /// Construct a [`Message`] from an [`ErrorResponse`].
  #[inline(always)]
  pub const fn error_response(val: ErrorResponse) -> Self {
    Self::ErrorResponse(val)
  }
}
