use std::env::var;

fn main() {
  // Don't rerun this on changes other than build.rs, as we only depend on
  // the rustc version.
  println!("cargo:rerun-if-changed=build.rs");

  let tarpaulin = var("CARGO_TARPAULIN").is_ok();

  if tarpaulin {
    use_feature("tarpaulin");
  }

  // Rerun this script if any of our features or configuration flags change,
  // or if the toolchain we used for feature detection changes.
  println!("cargo:rerun-if-env-changed=CARGO_TARPAULIN");
}

fn use_feature(feature: &str) {
  println!("cargo:rustc-cfg={}", feature);
}
