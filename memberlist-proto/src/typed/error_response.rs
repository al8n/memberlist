use core::fmt;
use smol_str::SmolStr;

/// Error response from the remote peer
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ErrorResponse {
  message: SmolStr,
}

impl ErrorResponse {
  /// Create a new error response
  #[inline(always)]
  pub fn new(message: impl Into<SmolStr>) -> Self {
    Self {
      message: message.into(),
    }
  }

  /// Returns the message of the error response as a string slice.
  #[inline(always)]
  pub fn message(&self) -> &str {
    self.message.as_str()
  }

  /// Sets the msg of the error response
  #[inline(always)]
  pub fn set_message(&mut self, msg: impl Into<SmolStr>) -> &mut Self {
    self.message = msg.into();
    self
  }

  /// Sets the msg of the error response (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub fn with_message(mut self, msg: impl Into<SmolStr>) -> Self {
    self.message = msg.into();
    self
  }
}

impl fmt::Display for ErrorResponse {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.message)
  }
}

impl core::error::Error for ErrorResponse {}

impl From<ErrorResponse> for SmolStr {
  fn from(err: ErrorResponse) -> Self {
    err.message
  }
}

impl From<SmolStr> for ErrorResponse {
  fn from(msg: SmolStr) -> Self {
    Self { message: msg }
  }
}
