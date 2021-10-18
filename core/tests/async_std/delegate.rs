use super::*;

#[test]
fn delegate_meta() {
  run(test_delegate_meta::<AsyncStdRuntime>());
}

#[test]
fn delegate_meta_update() {
  run(test_delegate_meta_update::<AsyncStdRuntime>());
}

#[test]
fn user_data() {
  run(test_user_data::<AsyncStdRuntime>());
}

#[test]
fn ping_delegate() {
  run(test_ping_delegate::<AsyncStdRuntime>());
}
