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
/// use memberlist_core::util::hostname;
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
      Err(_) => None,
    }
  };

  None
}
