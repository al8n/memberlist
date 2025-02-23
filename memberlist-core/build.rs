use std::env::var;

fn main() {
  // Don't rerun this on changes other than build.rs, as we only depend on
  // the rustc version.
  println!("cargo:rerun-if-changed=build.rs");

  // Check for `--features=crc32`.
  let crc32 = var("CARGO_FEATURE_CRC32").is_ok();
  // Check for `--features=xxhash64`.
  let xxhash64 = var("CARGO_FEATURE_XXHASH64").is_ok();
  // Check for `--features=xxhash32`.
  let xxhash32 = var("CARGO_FEATURE_XXHASH32").is_ok();
  // Check for `--features=xxhash3`.
  let xxhash3 = var("CARGO_FEATURE_XXHASH3").is_ok();
  // Check for `--features=murmur3`.
  let murmur3 = var("CARGO_FEATURE_MURMUR3").is_ok();

  let checksum = crc32 || xxhash64 || xxhash32 || xxhash3 || murmur3;

  // Check for `--features=zstd`.
  let zstd = var("CARGO_FEATURE_ZSTD").is_ok();
  // Check for `--features=lz4`.
  let lz4 = var("CARGO_FEATURE_LZ4").is_ok();
  // Check for `--features=snappy`.
  let snappy = var("CARGO_FEATURE_SNAPPY").is_ok();
  // Check for `--features=brotli`.
  let brotli = var("CARGO_FEATURE_BROTLI").is_ok();

  // Check for `--features=encryption`.
  let encryption = var("CARGO_FEATURE_ENCRYPTION").is_ok();

  let compression = zstd || lz4 || snappy || brotli;

  let offload = compression || encryption;

  if compression {
    use_feature("compression");
  }

  if offload {
    use_feature("offload");
  }

  if checksum {
    use_feature("checksum");
  }

  // Rerun this script if any of our features or configuration flags change,
  // or if the toolchain we used for feature detection changes.
  println!("cargo:rerun-if-env-changed=CARGO_FEATURE_CRC32");
  println!("cargo:rerun-if-env-changed=CARGO_FEATURE_XXHASH64");
  println!("cargo:rerun-if-env-changed=CARGO_FEATURE_XXHASH32");
  println!("cargo:rerun-if-env-changed=CARGO_FEATURE_XXHASH3");
  println!("cargo:rerun-if-env-changed=CARGO_FEATURE_MURMUR3");
  println!("cargo:rerun-if-env-changed=CARGO_FEATURE_ZSTD");
  println!("cargo:rerun-if-env-changed=CARGO_FEATURE_LZ4");
  println!("cargo:rerun-if-env-changed=CARGO_FEATURE_SNAPPY");
  println!("cargo:rerun-if-env-changed=CARGO_FEATURE_BROTLI");
  println!("cargo:rerun-if-env-changed=CARGO_FEATURE_ENCRYPTION");
}

fn use_feature(feature: &str) {
  println!("cargo:rustc-cfg={}", feature);
}
