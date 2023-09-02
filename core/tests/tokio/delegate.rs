use super::*;

#[test]
fn delegate_meta() {
  run(test_delegate_meta::<TokioRuntime>());
}

#[test]
fn delegate_meta_update() {
  run(test_delegate_meta_update::<TokioRuntime>());
}

#[test]
fn user_data() {
  run(test_user_data::<TokioRuntime>());
}

#[test]
fn ping_delegate() {
  run(test_ping_delegate::<TokioRuntime>());
}
