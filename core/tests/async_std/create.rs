use super::*;

#[test]
fn create_secret_key() {
  run(test_create_secret_key::<AsyncStdRuntime>());
}

#[test]
fn create_secret_key_empty() {
  run(test_create_secret_key_empty::<AsyncStdRuntime>());
}

#[test]
fn create_keyring_only() {
  run(test_create_keyring_only::<AsyncStdRuntime>());
}

#[test]
fn create_keyring_and_primary_key() {
  run(test_create_keyring_and_primary_key::<AsyncStdRuntime>());
}

#[test]
fn create() {
  run(test_create::<AsyncStdRuntime>());
}

#[test]
fn advertise_addr() {
  run(test_advertise_addr::<AsyncStdRuntime>());
}
