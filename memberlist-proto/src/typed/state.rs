use std::borrow::Cow;

/// State for the memberlist
#[derive(
  Debug, Default, Copy, Clone, PartialEq, Eq, Hash, derive_more::IsVariant, derive_more::Display,
)]
#[non_exhaustive]
pub enum State {
  /// Alive state
  #[default]
  #[display("alive")]
  Alive,
  /// Suspect state
  #[display("suspect")]
  Suspect,
  /// Dead state
  #[display("dead")]
  Dead,
  /// Left state
  #[display("left")]
  Left,
  /// Unknown state (used for forwards and backwards compatibility)
  #[display("unknown({_0})")]
  Unknown(u8),
}

impl From<u8> for State {
  fn from(value: u8) -> Self {
    match value {
      0 => Self::Alive,
      1 => Self::Suspect,
      2 => Self::Dead,
      3 => Self::Left,
      val => Self::Unknown(val),
    }
  }
}

impl From<State> for u8 {
  fn from(value: State) -> Self {
    match value {
      State::Alive => 0,
      State::Suspect => 1,
      State::Dead => 2,
      State::Left => 3,
      State::Unknown(val) => val,
    }
  }
}

impl State {
  /// Returns the [`State`] as a str representation.
  #[inline(always)]
  pub fn as_str(&self) -> Cow<'static, str> {
    match self {
      Self::Alive => Cow::Borrowed("alive"),
      Self::Suspect => Cow::Borrowed("suspect"),
      Self::Dead => Cow::Borrowed("dead"),
      Self::Left => Cow::Borrowed("left"),
      Self::Unknown(val) => Cow::Owned(format!("unknown({val})")),
    }
  }
}
