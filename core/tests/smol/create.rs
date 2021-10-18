use super::*;

#[test]
fn create_secret_key() {
  run(test_create_secret_key::<SmolRuntime>());
}

#[test]
fn create_secret_key_empty() {
  run(test_create_secret_key_empty::<SmolRuntime>());
}

#[test]
fn create_keyring_only() {
  run(test_create_keyring_only::<SmolRuntime>());
}

#[test]
fn create_keyring_and_primary_key() {
  run(test_create_keyring_and_primary_key::<SmolRuntime>());
}

#[test]
fn create() {
  run(test_create::<SmolRuntime>());
}
