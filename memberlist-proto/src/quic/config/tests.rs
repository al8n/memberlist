use super::*;
use crate::tls::ClientAuthMode;
use std::{
  path::Path,
  sync::atomic::{AtomicU64, Ordering},
};

/// Install the ring provider once for the whole test binary. Idempotent.
fn install_provider() {
  // Ignoring Err: a non-first install returns the existing provider; we only
  // need *some* default installed.
  let _ = rustls::crypto::ring::default_provider().install_default();
}

/// A unique temp directory for one test.
fn unique_dir() -> PathBuf {
  static COUNTER: AtomicU64 = AtomicU64::new(0);
  let n = COUNTER.fetch_add(1, Ordering::Relaxed);
  let dir = std::env::temp_dir().join(format!(
    "memberlist-quic-config-{}-{}",
    std::process::id(),
    n
  ));
  std::fs::create_dir_all(&dir).unwrap();
  dir
}

/// Write a self-signed cert + key (also used as the CA) and return the paths.
fn write_self_signed(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
  let cert_pem = ck.cert.pem();
  let key_pem = ck.signing_key.serialize_pem();

  let cert_path = dir.join("cert.pem");
  let key_path = dir.join("key.pem");
  let ca_path = dir.join("ca.pem");

  std::fs::write(&cert_path, &cert_pem).unwrap();
  std::fs::write(&key_path, &key_pem).unwrap();
  std::fs::write(&ca_path, &cert_pem).unwrap();

  (cert_path, key_path, ca_path)
}

#[test]
fn config_options_accessors_reflect_construction() {
  // These accessors are otherwise only reached behind the serde / clap feature
  // gates; assert them directly so they hold without either feature.
  let opts = QuicConfigOptions::new(
    PathBuf::from("/etc/node.pem"),
    PathBuf::from("/etc/node.key"),
    PathBuf::from("/etc/ca.pem"),
  );
  assert_eq!(opts.cert_file(), &PathBuf::from("/etc/node.pem"));
  assert_eq!(opts.key_file(), &PathBuf::from("/etc/node.key"));
  assert_eq!(opts.ca_file(), &PathBuf::from("/etc/ca.pem"));
  // `new` installs the documented defaults.
  assert_eq!(opts.client_auth(), ClientAuthMode::ClusterCa);
  assert_eq!(opts.max_idle_timeout(), None);
  assert_eq!(opts.keep_alive_interval(), None);
  assert_eq!(opts.unreliable_transport(), UnreliableTransport::Datagram);

  // Every builder override is observable through its accessor.
  let opts = opts
    .with_client_auth(ClientAuthMode::TrustedNetwork)
    .with_max_idle_timeout(Some(Duration::from_secs(20)))
    .with_keep_alive_interval(Some(Duration::from_secs(5)))
    .with_unreliable_transport(UnreliableTransport::Udp);
  assert_eq!(opts.client_auth(), ClientAuthMode::TrustedNetwork);
  assert_eq!(opts.max_idle_timeout(), Some(Duration::from_secs(20)));
  assert_eq!(opts.keep_alive_interval(), Some(Duration::from_secs(5)));
  assert_eq!(opts.unreliable_transport(), UnreliableTransport::Udp);
}

#[test]
fn build_installs_configured_server_name() {
  install_provider();
  let dir = unique_dir();
  let (cert, key, ca) = write_self_signed(&dir);

  // `with_server_name` flows through `build` into `QuicOptions::new`, which
  // installs it as the cluster-uniform SNI returned for every peer.
  let opts = QuicConfigOptions::new(cert, key, ca).with_server_name("peer.example");
  let cfg = opts.build().expect("build should succeed");
  let peer = "203.0.113.7:7946".parse().unwrap();
  assert_eq!(&*cfg.sni_for(&peer), "peer.example");

  // The default name is installed when `with_server_name` is not called.
  let dir = unique_dir();
  let (cert, key, ca) = write_self_signed(&dir);
  let cfg = QuicConfigOptions::new(cert, key, ca)
    .build()
    .expect("build should succeed");
  assert_eq!(&*cfg.sni_for(&peer), "localhost");
}

#[cfg(feature = "serde")]
#[test]
fn quic_config_options_serde_round_trip() {
  let opts = QuicConfigOptions::new(
    PathBuf::from("/etc/certs/node.pem"),
    PathBuf::from("/etc/certs/node.key"),
    PathBuf::from("/etc/certs/ca.pem"),
  )
  .with_client_auth(ClientAuthMode::TrustedNetwork)
  .with_max_idle_timeout(Some(Duration::from_secs(30)))
  .with_keep_alive_interval(Some(Duration::from_secs(10)))
  .with_unreliable_transport(UnreliableTransport::Udp);
  let j = serde_json::to_string(&opts).unwrap();
  let back: QuicConfigOptions = serde_json::from_str(&j).unwrap();
  assert_eq!(opts, back);
  assert_eq!(back.client_auth(), ClientAuthMode::TrustedNetwork);
  assert_eq!(back.max_idle_timeout(), Some(Duration::from_secs(30)));
  assert_eq!(back.keep_alive_interval(), Some(Duration::from_secs(10)));
  assert_eq!(back.unreliable_transport(), UnreliableTransport::Udp);
}

#[cfg(feature = "serde")]
#[test]
fn quic_config_options_serde_partial_defaults() {
  // Every tuning field is `serde(default)`; only the paths are required.
  let j = r#"{"cert_file":"/c.pem","key_file":"/k.pem","ca_file":"/ca.pem"}"#;
  let opts: QuicConfigOptions = serde_json::from_str(j).unwrap();
  assert_eq!(opts.client_auth(), ClientAuthMode::ClusterCa);
  assert_eq!(opts.max_idle_timeout(), None);
  assert_eq!(opts.keep_alive_interval(), None);
  assert_eq!(opts.unreliable_transport(), UnreliableTransport::Datagram);
}

#[cfg(feature = "serde")]
#[test]
fn quic_config_options_serde_humantime_strings() {
  // humantime renders the durations as strings; confirm a string config parses.
  let j = r#"{
    "cert_file":"/c.pem","key_file":"/k.pem","ca_file":"/ca.pem",
    "max_idle_timeout":"15s","keep_alive_interval":"5s"
  }"#;
  let opts: QuicConfigOptions = serde_json::from_str(j).unwrap();
  assert_eq!(opts.max_idle_timeout(), Some(Duration::from_secs(15)));
  assert_eq!(opts.keep_alive_interval(), Some(Duration::from_secs(5)));
}

#[cfg(feature = "serde")]
#[test]
fn quic_config_options_serde_missing_required_path_errors() {
  let j = r#"{"key_file":"/k.pem","ca_file":"/ca.pem"}"#;
  assert!(serde_json::from_str::<QuicConfigOptions>(j).is_err());
}

#[cfg(feature = "clap")]
#[test]
fn quic_config_options_clap_parse_and_env_ids() {
  use clap::{CommandFactory, Parser};

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    quic: QuicConfigOptions,
  }

  let cli = Cli::try_parse_from([
    "prog",
    "--quic-cert",
    "/c.pem",
    "--quic-key",
    "/k.pem",
    "--quic-ca",
    "/ca.pem",
    "--quic-client-auth",
    "trusted-network",
    "--quic-max-idle-timeout",
    "30s",
    "--quic-keep-alive-interval",
    "10s",
    "--quic-unreliable-transport",
    "udp",
  ])
  .unwrap();
  assert_eq!(cli.quic.cert_file(), &PathBuf::from("/c.pem"));
  assert_eq!(cli.quic.client_auth(), ClientAuthMode::TrustedNetwork);
  assert_eq!(cli.quic.max_idle_timeout(), Some(Duration::from_secs(30)));
  assert_eq!(
    cli.quic.keep_alive_interval(),
    Some(Duration::from_secs(10))
  );
  assert_eq!(cli.quic.unreliable_transport(), UnreliableTransport::Udp);

  // Defaults when only the required paths are supplied.
  let cli = Cli::try_parse_from([
    "prog",
    "--quic-cert",
    "/c",
    "--quic-key",
    "/k",
    "--quic-ca",
    "/ca",
  ])
  .unwrap();
  assert_eq!(cli.quic.client_auth(), ClientAuthMode::ClusterCa);
  assert_eq!(cli.quic.max_idle_timeout(), None);
  assert_eq!(
    cli.quic.unreliable_transport(),
    UnreliableTransport::Datagram
  );

  let cmd = Cli::command();
  let env_vars: Vec<_> = cmd
    .get_arguments()
    .filter_map(|a| a.get_env().and_then(|e| e.to_str()))
    .collect();
  for v in [
    "MEMBERLIST_QUIC_CERT",
    "MEMBERLIST_QUIC_KEY",
    "MEMBERLIST_QUIC_CA",
    "MEMBERLIST_QUIC_CLIENT_AUTH",
    "MEMBERLIST_QUIC_MAX_IDLE_TIMEOUT",
    "MEMBERLIST_QUIC_KEEP_ALIVE_INTERVAL",
    "MEMBERLIST_QUIC_UNRELIABLE_TRANSPORT",
  ] {
    assert!(env_vars.contains(&v), "missing env var {v}");
  }
}

#[cfg(feature = "clap")]
#[test]
fn quic_config_options_clap_partial_update_preserves_defaulted_fields() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    quic: QuicConfigOptions,
  }

  // A base whose three defaulted fields all hold NON-default values.
  let mut cli = Cli {
    quic: QuicConfigOptions::new(
      PathBuf::from("/c.pem"),
      PathBuf::from("/k.pem"),
      PathBuf::from("/ca.pem"),
    )
    .with_client_auth(ClientAuthMode::TrustedNetwork)
    .with_server_name("peer.example")
    .with_unreliable_transport(UnreliableTransport::Udp),
  };

  // An update overriding only the cert path must leave all three defaulted
  // fields at their non-default values rather than snapping back to defaults.
  cli
    .try_update_from(["prog", "--quic-cert", "/new-cert.pem"])
    .expect("partial update succeeds");
  assert_eq!(
    cli.quic.client_auth(),
    ClientAuthMode::TrustedNetwork,
    "non-default client_auth must survive a partial update"
  );
  assert_eq!(
    cli.quic.unreliable_transport(),
    UnreliableTransport::Udp,
    "non-default unreliable_transport must survive a partial update"
  );
  assert_eq!(cli.quic.cert_file(), &PathBuf::from("/new-cert.pem"));

  // An explicit override of a defaulted field IS applied.
  cli
    .try_update_from(["prog", "--quic-client-auth", "cluster-ca"])
    .expect("override update succeeds");
  assert_eq!(cli.quic.client_auth(), ClientAuthMode::ClusterCa);
  // ...and the still-unset defaulted fields remain untouched.
  assert_eq!(cli.quic.unreliable_transport(), UnreliableTransport::Udp);
}

/// Build the bundle and prove it drives a `quinn_proto::Endpoint`.
fn assert_usable(opts: &QuicConfigOptions) {
  let cfg = opts.build().expect("build should succeed");
  let _server_ep =
    quinn_proto::Endpoint::new(cfg.endpoint_arc(), Some(cfg.server_arc()), true, None);
  let _client_ep = quinn_proto::Endpoint::new(cfg.endpoint_arc(), None, true, None);
}

#[test]
fn build_cluster_ca_produces_usable_options() {
  install_provider();
  let dir = unique_dir();
  let (cert, key, ca) = write_self_signed(&dir);
  let opts = QuicConfigOptions::new(cert, key, ca).with_client_auth(ClientAuthMode::ClusterCa);
  assert_usable(&opts);
  let cfg = opts.build().unwrap();
  assert_eq!(cfg.unreliable_transport(), UnreliableTransport::Datagram);
}

#[test]
fn build_trusted_network_produces_usable_options() {
  install_provider();
  let dir = unique_dir();
  let (cert, key, ca) = write_self_signed(&dir);
  let opts = QuicConfigOptions::new(cert, key, ca)
    .with_client_auth(ClientAuthMode::TrustedNetwork)
    .with_unreliable_transport(UnreliableTransport::Udp);
  assert_usable(&opts);
  let cfg = opts.build().unwrap();
  assert_eq!(cfg.unreliable_transport(), UnreliableTransport::Udp);
}

#[test]
fn build_with_timeout_tuning_is_usable() {
  install_provider();
  let dir = unique_dir();
  let (cert, key, ca) = write_self_signed(&dir);
  let opts = QuicConfigOptions::new(cert, key, ca)
    .with_max_idle_timeout(Some(Duration::from_secs(30)))
    .with_keep_alive_interval(Some(Duration::from_secs(10)));
  assert_usable(&opts);
}

#[test]
fn build_missing_cert_file_errors() {
  install_provider();
  let dir = unique_dir();
  let (_cert, key, ca) = write_self_signed(&dir);
  let opts = QuicConfigOptions::new(dir.join("missing.pem"), key, ca);
  // `QuicOptions` is not `Debug`, so destructure rather than `unwrap_err`.
  let Err(err) = opts.build() else {
    panic!("missing cert file must fail to build");
  };
  assert!(
    matches!(err, QuicConfigError::Tls(TlsConfigError::ReadFile(_, _))),
    "expected Tls(ReadFile), got {err:?}"
  );
}
