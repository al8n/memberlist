use metrics::Label;

smallvec_wrapper::smallvec_wrapper!(
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

  use serde::{Deserialize, Serialize, ser::SerializeMap};

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
