mod harness;

#[test]
fn link_builds() {
  let (_a, _b) = harness::link(1500);
  let _c = harness::Clock::new();
}
