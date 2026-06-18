//! Emits the aggregate transform cfgs (`compression` / `encryption` / `checksum`),
//! each set when any of its backend features is enabled, so transform code gates
//! on `#[cfg(compression)]` etc. instead of repeating the full backend list.

fn any_feature(names: &[&str]) -> bool {
  names
    .iter()
    .any(|name| std::env::var_os(format!("CARGO_FEATURE_{name}")).is_some())
}

fn main() {
  println!("cargo::rustc-check-cfg=cfg(compression)");
  println!("cargo::rustc-check-cfg=cfg(encryption)");
  println!("cargo::rustc-check-cfg=cfg(checksum)");
  if any_feature(&["LZ4", "SNAPPY", "ZSTD", "BROTLI"]) {
    println!("cargo::rustc-cfg=compression");
  }
  if any_feature(&["AES_GCM", "CHACHA20_POLY1305"]) {
    println!("cargo::rustc-cfg=encryption");
  }
  if any_feature(&["CRC32", "XXHASH32", "XXHASH64", "XXHASH3", "MURMUR3"]) {
    println!("cargo::rustc-cfg=checksum");
  }
}
