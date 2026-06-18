//! Build-time codegen for the `messages` module, plus the aggregate transform
//! cfgs.
//!
//! Uses `buffa-build` to invoke `protoc` against
//! `proto/memberlist/v1/messages.proto` and produce Rust types via
//! `buffa-codegen`. The output is written to `OUT_DIR` and pulled in by
//! `src/messages.rs` via `include!`.
//!
//! It also derives three aggregate cfgs — `compression`, `encryption`, and
//! `checksum` — each set when ANY of its backend features is enabled. The
//! transform-pipeline code gates on `#[cfg(compression)]` (etc.) instead of
//! repeating the full `any(feature = "lz4", …)` backend list at every site.
//! These are build-script cfgs rather than Cargo features on purpose: a
//! capability that is meaningless without a concrete backend should not be
//! separately enableable, and it must not appear as a phantom feature in the
//! crate's docs.rs feature list. The public-facing items keep an explicit
//! `#[cfg_attr(docsrs, doc(cfg(any(feature = "lz4", …))))]`, so docs.rs still
//! renders the real backend features in the gate badge.

/// True when any of the named Cargo features (the `CARGO_FEATURE_<NAME>` env
/// vars Cargo sets for enabled features) is active for this build.
fn any_feature(names: &[&str]) -> bool {
  names
    .iter()
    .any(|name| std::env::var_os(format!("CARGO_FEATURE_{name}")).is_some())
}

fn main() {
  println!("cargo:rerun-if-changed=build.rs");
  println!("cargo:rerun-if-changed=proto");

  // Register the aggregate cfgs so `#[cfg(compression)]` (etc.) is not flagged
  // by the `unexpected_cfgs` lint, then emit each when a backend selects it.
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

  buffa_build::Config::new()
    .files(&["proto/memberlist/v1/messages.proto"])
    .includes(&["proto"])
    .use_bytes_type()
    .include_file("memberlist_wire_generated.rs")
    .compile()
    .expect("buffa codegen failed");
}
