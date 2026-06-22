use super::*;
use std::{
  path::PathBuf,
  sync::atomic::{AtomicU64, Ordering},
};

/// Install the ring provider once for the whole test binary. Idempotent: a
/// second install is a no-op (the returned `Err` carries the already-installed
/// provider, which we ignore).
fn install_provider() {
  // Ignoring Err: a non-first install returns the existing provider; we only
  // need *some* default installed.
  let _ = rustls::crypto::ring::default_provider().install_default();
}

/// A unique temp directory for one test, so concurrent tests never collide on a
/// fixed filename.
fn unique_dir() -> PathBuf {
  static COUNTER: AtomicU64 = AtomicU64::new(0);
  let n = COUNTER.fetch_add(1, Ordering::Relaxed);
  let dir = std::env::temp_dir().join(format!(
    "memberlist-tls-config-{}-{}",
    std::process::id(),
    n
  ));
  std::fs::create_dir_all(&dir).unwrap();
  dir
}

/// Write a self-signed cert + key to `cert.pem` / `key.pem` in `dir`, using the
/// same cert as the CA. Returns the three paths.
fn write_self_signed(dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf) {
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
  let cert_pem = ck.cert.pem();
  let key_pem = ck.signing_key.serialize_pem();

  let cert_path = dir.join("cert.pem");
  let key_path = dir.join("key.pem");
  let ca_path = dir.join("ca.pem");

  std::fs::write(&cert_path, &cert_pem).unwrap();
  std::fs::write(&key_path, &key_pem).unwrap();
  // The self-signed cert is its own CA for the test.
  std::fs::write(&ca_path, &cert_pem).unwrap();

  (cert_path, key_path, ca_path)
}

#[test]
fn client_auth_mode_str_round_trip() {
  for mode in [ClientAuthMode::ClusterCa, ClientAuthMode::TrustedNetwork] {
    let s = mode.as_str();
    assert_eq!(s.parse::<ClientAuthMode>().unwrap(), mode);
  }
  assert_eq!(ClientAuthMode::ClusterCa.as_str(), "cluster-ca");
  assert_eq!(ClientAuthMode::TrustedNetwork.as_str(), "trusted-network");
  assert!("nope".parse::<ClientAuthMode>().is_err());
  assert_eq!(ClientAuthMode::default(), ClientAuthMode::ClusterCa);
}

#[test]
fn config_options_accessors_reflect_construction() {
  // The path + auth accessors are otherwise only reached behind the serde / clap
  // feature gates; assert them directly so they hold without either feature.
  let opts = TlsConfigOptions::new(
    PathBuf::from("/etc/node.pem"),
    PathBuf::from("/etc/node.key"),
    PathBuf::from("/etc/ca.pem"),
  );
  assert_eq!(opts.cert_file(), &PathBuf::from("/etc/node.pem"));
  assert_eq!(opts.key_file(), &PathBuf::from("/etc/node.key"));
  assert_eq!(opts.ca_file(), &PathBuf::from("/etc/ca.pem"));
  // `new` installs the default (mutual TLS) auth mode.
  assert_eq!(opts.client_auth(), ClientAuthMode::ClusterCa);

  // The builder override is observable through the accessor.
  let opts = opts.with_client_auth(ClientAuthMode::TrustedNetwork);
  assert_eq!(opts.client_auth(), ClientAuthMode::TrustedNetwork);
}

#[cfg(feature = "serde")]
#[test]
fn client_auth_mode_serde_kebab_case() {
  let j = serde_json::to_string(&ClientAuthMode::TrustedNetwork).unwrap();
  assert_eq!(j, "\"trusted-network\"");
  let back: ClientAuthMode = serde_json::from_str("\"cluster-ca\"").unwrap();
  assert_eq!(back, ClientAuthMode::ClusterCa);
}

#[cfg(feature = "serde")]
#[test]
fn tls_config_options_serde_round_trip() {
  let opts = TlsConfigOptions::new(
    PathBuf::from("/etc/certs/node.pem"),
    PathBuf::from("/etc/certs/node.key"),
    PathBuf::from("/etc/certs/ca.pem"),
  )
  .with_client_auth(ClientAuthMode::TrustedNetwork);
  let j = serde_json::to_string(&opts).unwrap();
  let back: TlsConfigOptions = serde_json::from_str(&j).unwrap();
  assert_eq!(opts, back);
  assert_eq!(back.client_auth(), ClientAuthMode::TrustedNetwork);
}

#[cfg(feature = "serde")]
#[test]
fn tls_config_options_serde_partial_defaults_client_auth() {
  // client_auth is `serde(default)` — omitting it yields ClusterCa.
  let j = r#"{"cert_file":"/c.pem","key_file":"/k.pem","ca_file":"/ca.pem"}"#;
  let opts: TlsConfigOptions = serde_json::from_str(j).unwrap();
  assert_eq!(opts.client_auth(), ClientAuthMode::ClusterCa);
  assert_eq!(opts.cert_file(), &PathBuf::from("/c.pem"));
}

#[cfg(feature = "serde")]
#[test]
fn tls_config_options_serde_missing_required_path_errors() {
  // cert_file is required (no serde default) — omission is an error.
  let j = r#"{"key_file":"/k.pem","ca_file":"/ca.pem"}"#;
  assert!(serde_json::from_str::<TlsConfigOptions>(j).is_err());
}

#[cfg(feature = "clap")]
#[test]
fn tls_config_options_clap_parse_and_env_ids() {
  use clap::{CommandFactory, Parser};

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    tls: TlsConfigOptions,
  }

  let cli = Cli::try_parse_from([
    "prog",
    "--tls-cert",
    "/c.pem",
    "--tls-key",
    "/k.pem",
    "--tls-ca",
    "/ca.pem",
    "--tls-client-auth",
    "trusted-network",
  ])
  .unwrap();
  assert_eq!(cli.tls.cert_file(), &PathBuf::from("/c.pem"));
  assert_eq!(cli.tls.key_file(), &PathBuf::from("/k.pem"));
  assert_eq!(cli.tls.ca_file(), &PathBuf::from("/ca.pem"));
  assert_eq!(cli.tls.client_auth(), ClientAuthMode::TrustedNetwork);

  // Defaulted client_auth when the flag is omitted.
  let cli = Cli::try_parse_from([
    "prog",
    "--tls-cert",
    "/c",
    "--tls-key",
    "/k",
    "--tls-ca",
    "/ca",
  ])
  .unwrap();
  assert_eq!(cli.tls.client_auth(), ClientAuthMode::ClusterCa);

  // The env vars are wired onto the args.
  let cmd = Cli::command();
  let env_vars: Vec<_> = cmd
    .get_arguments()
    .filter_map(|a| a.get_env().and_then(|e| e.to_str()))
    .collect();
  assert!(env_vars.contains(&"MEMBERLIST_TLS_CERT"));
  assert!(env_vars.contains(&"MEMBERLIST_TLS_KEY"));
  assert!(env_vars.contains(&"MEMBERLIST_TLS_CA"));
  assert!(env_vars.contains(&"MEMBERLIST_TLS_CLIENT_AUTH"));
}

#[cfg(feature = "clap")]
#[test]
fn tls_config_options_clap_partial_update_preserves_client_auth() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    tls: TlsConfigOptions,
  }

  // A base whose `client_auth` is the NON-default TrustedNetwork.
  let mut cli = Cli {
    tls: TlsConfigOptions::new(
      PathBuf::from("/c.pem"),
      PathBuf::from("/k.pem"),
      PathBuf::from("/ca.pem"),
    )
    .with_client_auth(ClientAuthMode::TrustedNetwork),
  };

  // An update overriding only the cert path must leave the defaulted
  // `client_auth` as TrustedNetwork rather than snapping back to ClusterCa.
  cli
    .try_update_from(["prog", "--tls-cert", "/new-cert.pem"])
    .expect("partial update succeeds");
  assert_eq!(
    cli.tls.client_auth(),
    ClientAuthMode::TrustedNetwork,
    "non-default client_auth must survive a partial update"
  );
  assert_eq!(cli.tls.cert_file(), &PathBuf::from("/new-cert.pem"));

  // An explicit override of `client_auth` IS applied.
  cli
    .try_update_from(["prog", "--tls-client-auth", "cluster-ca"])
    .expect("override update succeeds");
  assert_eq!(cli.tls.client_auth(), ClientAuthMode::ClusterCa);
}

#[test]
fn build_cluster_ca_produces_usable_options() {
  install_provider();
  let dir = unique_dir();
  let (cert, key, ca) = write_self_signed(&dir);

  let opts = TlsConfigOptions::new(cert, key, ca).with_client_auth(ClientAuthMode::ClusterCa);
  let tls = opts.build().expect("cluster-ca build should succeed");

  // Both rustls connections construct from the assembled configs.
  let _server = rustls::ServerConnection::new(tls.server_arc()).unwrap();
  let _client = rustls::ClientConnection::new(
    tls.client_arc(),
    rustls::pki_types::ServerName::try_from("localhost").unwrap(),
  )
  .unwrap();
}

#[test]
fn build_trusted_network_produces_usable_options() {
  install_provider();
  let dir = unique_dir();
  let (cert, key, ca) = write_self_signed(&dir);

  let opts = TlsConfigOptions::new(cert, key, ca).with_client_auth(ClientAuthMode::TrustedNetwork);
  let tls = opts.build().expect("trusted-network build should succeed");

  let _server = rustls::ServerConnection::new(tls.server_arc()).unwrap();
  let _client = rustls::ClientConnection::new(
    tls.client_arc(),
    rustls::pki_types::ServerName::try_from("localhost").unwrap(),
  )
  .unwrap();
}

#[test]
fn build_missing_cert_file_errors() {
  install_provider();
  let dir = unique_dir();
  let (_cert, key, ca) = write_self_signed(&dir);

  let opts = TlsConfigOptions::new(dir.join("does-not-exist.pem"), key, ca);
  // `TlsOptions` is not `Debug`, so destructure rather than `unwrap_err`.
  let Err(err) = opts.build() else {
    panic!("missing cert file must fail to build");
  };
  assert!(
    matches!(err, TlsConfigError::ReadFile(_, _)),
    "expected ReadFile, got {err:?}"
  );
}

#[test]
fn build_empty_cert_file_errors() {
  install_provider();
  let dir = unique_dir();
  let (_cert, key, ca) = write_self_signed(&dir);
  let empty = dir.join("empty.pem");
  std::fs::write(&empty, b"").unwrap();

  let opts = TlsConfigOptions::new(empty.clone(), key, ca);
  let Err(err) = opts.build() else {
    panic!("empty cert file must fail to build");
  };
  assert!(
    matches!(err, TlsConfigError::NoCerts(p) if p == empty),
    "expected NoCerts for the empty cert file"
  );
}
