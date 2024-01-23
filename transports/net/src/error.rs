/// Connection kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ConnectionKind {
  /// Promised connection, e.g. TCP, QUIC.
  Promised,
  /// Packet connection, e.g. UDP.
  Packet,
}

impl core::fmt::Display for ConnectionKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl ConnectionKind {
  /// Returns a string representation of the connection kind.
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      ConnectionKind::Promised => "promised",
      ConnectionKind::Packet => "packet",
    }
  }
}

/// Connection error kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ConnectionErrorKind {
  /// Failed to accept a connection.
  Accept,
  /// Failed to close a connection.
  Close,
  /// Failed to dial a connection.
  Dial,
  /// Failed to flush a connection.
  Flush,
  /// Failed to read from a connection.
  Read,
  /// Failed to write to a connection.
  Write,
  /// Failed to set a label on a connection.
  Label,
}

impl core::fmt::Display for ConnectionErrorKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl ConnectionErrorKind {
  /// Returns a string representation of the error kind.
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

/// Connection error.
#[derive(Debug)]
pub struct ConnectionError {
  pub(crate) kind: ConnectionKind,
  pub(crate) error_kind: ConnectionErrorKind,
  pub(crate) error: std::io::Error,
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

impl From<ConnectionError> for std::io::Error {
  fn from(value: ConnectionError) -> Self {
    value.error
  }
}

impl ConnectionError {
  /// Returns true if the error is a remote failure.
  #[inline]
  pub fn is_remote_failure(&self) -> bool {
    #[allow(clippy::match_like_matches_macro)]
    match self.kind {
      ConnectionKind::Promised => match self.error_kind {
        ConnectionErrorKind::Read | ConnectionErrorKind::Write | ConnectionErrorKind::Dial => true,
        _ => false,
      },
      ConnectionKind::Packet => match self.error_kind {
        ConnectionErrorKind::Write => true,
        _ => false,
      },
    }
  }

  /// Returns true if the error is unexpected EOF
  #[inline(always)]
  pub fn is_eof(&self) -> bool {
    self.error.kind() == std::io::ErrorKind::UnexpectedEof
  }

  pub(super) fn promised_read(err: std::io::Error) -> Self {
    Self {
      kind: ConnectionKind::Promised,
      error_kind: ConnectionErrorKind::Read,
      error: err,
    }
  }

  pub(super) fn promised_write(err: std::io::Error) -> Self {
    Self {
      kind: ConnectionKind::Promised,
      error_kind: ConnectionErrorKind::Write,
      error: err,
    }
  }
}
