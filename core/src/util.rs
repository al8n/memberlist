pub(crate) fn retransmit_limit(retransmit_mult: usize, n: usize) -> usize {
  let node_scale = ((n + 1) as f64).log10().ceil() as usize;
  retransmit_mult * node_scale
}

/// Returns the hostname of the current machine.
///
/// On wasm target, this function always returns `None`.
///
/// # Examples
///
/// ```
/// use showbiz_core::util::hostname;
///
/// let hostname = hostname();
/// println!("hostname: {hostname:?}");
/// ```
#[allow(unreachable_code)]
pub fn hostname() -> Option<String> {
  #[cfg(not(any(target_arch = "wasm32", windows)))]
  return {
    let name = rustix::system::uname();
    let name = name.nodename();
    name
      .is_empty()
      .then_some(name.to_string_lossy().to_string())
  };

  #[cfg(windows)]
  return {
    match ::hostname::get() {
      Ok(name) => {
        let name = name.to_string_lossy();
        name.is_empty().then_some(name.to_string())
      }
      Err(e) => None,
    }
  };

  None
}

#[cfg(feature = "metrics")]
pub mod label_serde {
  use std::{collections::HashMap, sync::Arc};

  use metrics::Label;
  use serde::{
    de::Deserializer,
    ser::{SerializeMap, Serializer},
    Deserialize,
  };

  pub fn serialize<S>(labels: &Arc<Vec<Label>>, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut ser = serializer.serialize_map(Some(labels.len()))?;
    for label in labels.iter() {
      ser.serialize_entry(label.key(), label.value())?;
    }
    ser.end()
  }

  pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<Vec<Label>>, D::Error>
  where
    D: Deserializer<'de>,
  {
    HashMap::<String, String>::deserialize(deserializer)
      .map(|map| Arc::new(map.into_iter().map(|(k, v)| Label::new(k, v)).collect()))
  }
}
