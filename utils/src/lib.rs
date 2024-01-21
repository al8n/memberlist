/// Network related utilities
pub mod net;

#[doc(hidden)]
pub mod __private {
  pub use smallvec;
}

/// Wraps a `SmallVec` with a newtype.
#[macro_export]
macro_rules! smallvec_wrapper {
  (
    $(#[$meta:meta])*
    $vis:vis $name:ident $(<$($generic:tt),+>)? ([$inner:ty; $inlined: expr]);
  ) => {
    $(#[$meta])*
    $vis struct $name $(< $($generic),+ >)? ($crate::__private::smallvec::SmallVec<[$inner; { $inlined }]>);

    impl $(< $($generic),+ >)? ::core::default::Default for $name $(< $($generic),+ >)? {
      fn default() -> Self {
        Self::new()
      }
    }

    impl $(< $($generic),+ >)? $name $(< $($generic),+ >)? {
      /// Creates a new instance.
      pub fn new() -> Self {
        Self($crate::__private::smallvec::SmallVec::new())
      }

      /// Creates a new instance of `ApplyBatchResponse` with the given capacity.
      pub fn with_capacity(capacity: usize) -> Self {
        Self($crate::__private::smallvec::SmallVec::with_capacity(capacity))
      }
    }

    impl $(< $($generic),+ >)? ::core::convert::From<$crate::__private::smallvec::SmallVec<[$inner; { $inlined }]>> for $name $(< $($generic),+ >)? {
      fn from(value: $crate::__private::smallvec::SmallVec<[$inner; { $inlined }]>) -> Self {
        Self(value)
      }
    }

    impl $(< $($generic),+ >)? ::core::ops::Deref for $name $(< $($generic),+ >)? {
      type Target = $crate::__private::smallvec::SmallVec<[$inner; { $inlined }]>;

      fn deref(&self) -> &Self::Target {
        &self.0
      }
    }

    impl $(< $($generic),+ >)? ::core::ops::DerefMut for $name $(< $($generic),+ >)? {
      fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
      }
    }

    impl $(< $($generic),+ >)? ::core::iter::FromIterator<$inner> for $name $(< $($generic),+ >)? {
      fn from_iter<__T: ::core::iter::IntoIterator<Item = $inner>>(iter: __T) -> Self {
        Self(iter.into_iter().collect())
      }
    }

    impl $(< $($generic),+ >)? ::core::iter::IntoIterator for $name $(< $($generic),+ >)? {
      type Item = $inner;
      type IntoIter = $crate::__private::smallvec::IntoIter<[$inner; { $inlined }]>;

      fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
      }
    }
  };
}

smallvec_wrapper!(
  /// A tiny vec which can inline 1 element on stack.
  #[derive(PartialEq, Eq, Hash, Clone, Debug, PartialOrd, Ord)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(transparent))]
  #[cfg_attr(
    feature = "rkyv",
    derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
  )]
  #[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
  pub OneOrMore<T>([T; 1]);
);

smallvec_wrapper!(
  /// A tiny vec which can inline 2 elements on stack.
  #[derive(PartialEq, Eq, Hash, Clone, Debug, PartialOrd, Ord)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(transparent))]
  #[cfg_attr(
    feature = "rkyv",
    derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
  )]
  #[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
  pub TinyVec<T>([T; 2]);
);

smallvec_wrapper!(
  /// A small vec which can inline 4 elements on stack.
  #[derive(PartialEq, Eq, Hash, Clone, Debug, PartialOrd, Ord)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(transparent))]
  #[cfg_attr(
    feature = "rkyv",
    derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
  )]
  #[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
  pub SmallVec<T>([T; 4]);
);

smallvec_wrapper!(
  /// A medium vec which can inline 8 elements on stack.
  #[derive(PartialEq, Eq, Hash, Clone, Debug, PartialOrd, Ord)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(transparent))]
  #[cfg_attr(
    feature = "rkyv",
    derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
  )]
  #[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
  pub MediumVec<T>([T; 8]);
);

smallvec_wrapper!(
  /// A big vec which can inline 16 elements on stack.
  #[derive(PartialEq, Eq, Hash, Clone, Debug, PartialOrd, Ord)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(transparent))]
  #[cfg_attr(
    feature = "rkyv",
    derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
  )]
  #[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
  pub BigVec<T>([T; 16]);
);

#[cfg(feature = "metrics")]
pub use _metrics::*;

#[cfg(feature = "metrics")]
mod _metrics {
  use metrics::Label;

  smallvec_wrapper!(
    /// A vector of `Label`s.
    #[derive(PartialEq, Eq, Hash, Clone, Debug, PartialOrd, Ord)]
    pub MetricLabels([Label; 2]);
  );

  impl metrics::IntoLabels for MetricLabels {
    fn into_labels(self) -> Vec<Label> {
      self.into_iter().collect()
    }
  }

  #[cfg(feature = "serde")]
  const _: () = {
    use std::collections::HashMap;

    use serde::{ser::SerializeMap, Deserialize, Serialize};

    impl Serialize for MetricLabels {
      fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
      where
        S: serde::Serializer,
      {
        let mut ser = serializer.serialize_map(Some(self.len()))?;
        for label in self.iter() {
          ser.serialize_entry(label.key(), label.value())?;
        }
        ser.end()
      }
    }

    impl<'de> Deserialize<'de> for MetricLabels {
      fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
      where
        D: serde::Deserializer<'de>,
      {
        HashMap::<String, String>::deserialize(deserializer)
          .map(|map| map.into_iter().map(|(k, v)| Label::new(k, v)).collect())
      }
    }
  };
}
