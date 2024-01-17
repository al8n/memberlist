#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ConnectionKind {
  Reliable,
  Unreliable,
}

impl core::fmt::Display for ConnectionKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl ConnectionKind {
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      ConnectionKind::Reliable => "reliable",
      ConnectionKind::Unreliable => "unreliable",
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ConnectionErrorKind {
  Accept,
  Close,
  Dial,
  Flush,
  Read,
  Write,
  Label,
}

impl core::fmt::Display for ConnectionErrorKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl ConnectionErrorKind {
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Accept => "accept",
      Self::Read => "read",
      Self::Write => "write",
      Self::Dial => "dial",
      Self::Flush => "flush",
      Self::Close => "close",
      Self::Label => "label",
    }
  }
}

#[viewit::viewit(vis_all = "pub")]
#[derive(Debug)]
pub struct ConnectionError {
  kind: ConnectionKind,
  error_kind: ConnectionErrorKind,
  error: std::io::Error,
}

impl core::fmt::Display for ConnectionError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "{} connection {} error {}",
      self.kind.as_str(),
      self.error_kind.as_str(),
      self.error
    )
  }
}

impl std::error::Error for ConnectionError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    Some(&self.error)
  }
}

impl ConnectionError {
  fn failed_remote(&self) -> bool {
    #[allow(clippy::match_like_matches_macro)]
    match self.kind {
      ConnectionKind::Reliable => match self.error_kind {
        ConnectionErrorKind::Read | ConnectionErrorKind::Write | ConnectionErrorKind::Dial => true,
        _ => false,
      },
      ConnectionKind::Unreliable => match self.error_kind {
        ConnectionErrorKind::Write => true,
        _ => false,
      },
    }
  }
}
