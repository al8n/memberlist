//! Build-time codegen for the `messages` module.
//!
//! Uses `buffa-build` to invoke `protoc` against
//! `proto/memberlist/v1/messages.proto` and produce Rust types via
//! `buffa-codegen`. The output is written to `OUT_DIR` and pulled in by
//! `src/messages.rs` via `include!`.

fn main() {
  println!("cargo:rerun-if-changed=build.rs");
  println!("cargo:rerun-if-changed=proto");

  buffa_build::Config::new()
    .files(&["proto/memberlist/v1/messages.proto"])
    .includes(&["proto"])
    .use_bytes_type()
    .string_type(buffa_build::StringRepr::SmolStr)
    .include_file("memberlist_wire_generated.rs")
    .compile()
    .expect("buffa codegen failed");
}
