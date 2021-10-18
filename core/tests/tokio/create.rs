use super::*;

#[test]
fn create_secret_key() {
  run(test_create_secret_key::<TokioRuntime>());
}

#[test]
fn create_secret_key_empty() {
  run(test_create_secret_key_empty::<TokioRuntime>());
}

#[test]
fn create_keyring_only() {
  run(test_create_keyring_only::<TokioRuntime>());
}

#[test]
fn create_keyring_and_primary_key() {
  run(test_create_keyring_and_primary_key::<TokioRuntime>());
}

#[test]
fn create() {
  run(test_create::<TokioRuntime>());
}

#[test]
fn advertise_addr() {
  run(test_advertise_addr::<TokioRuntime>());
}
