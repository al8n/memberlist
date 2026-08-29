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

#[test]
fn build_rejects_max_idle_timeout_that_encodes_to_zero() {
  install_provider();

  // A zero idle-timeout transport parameter DISABLES the QUIC idle timeout (RFC
  // 9000 §18.2), removing the stale-connection self-healing bound. quinn encodes
  // the timeout in milliseconds, so any sub-millisecond duration also rounds to a
  // disabling zero — all must be rejected. (`QuicOptions` is not `Debug`, so match
  // the `Result` rather than `expect_err`.)
  for t in [
    Duration::ZERO,
    Duration::from_nanos(1),
    Duration::from_micros(500),
    Duration::from_nanos(999_999),
  ] {
    let dir = unique_dir();
    let (cert, key, ca) = write_self_signed(&dir);
    let result = QuicConfigOptions::new(cert, key, ca)
      .with_max_idle_timeout(Some(t))
      .build();
    assert!(
      matches!(result, Err(QuicConfigError::IdleTimeoutZero)),
      "max_idle_timeout {t:?} encodes to a disabling zero and must be rejected"
    );
  }

  // A finite >= 1ms idle-timeout still builds.
  for t in [Duration::from_millis(1), Duration::from_secs(20)] {
    let dir = unique_dir();
    let (cert, key, ca) = write_self_signed(&dir);
    QuicConfigOptions::new(cert, key, ca)
      .with_max_idle_timeout(Some(t))
      .build()
      .expect("a finite (>= 1ms) max_idle_timeout builds");
  }
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
fn build_forces_early_data_off_on_both_sides() {
  install_provider();
  let dir = unique_dir();
  let (cert, key, ca) = write_self_signed(&dir);

  // The managed builder must force QUIC 0-RTT (early data) OFF on both rustls
  // configs it assembles, regardless of rustls's own defaults. Introspect the
  // pair BEFORE quinn's `try_from` wrapping hides those fields.
  let (server, client) = QuicConfigOptions::new(cert, key, ca)
    .assemble_rustls_configs()
    .expect("assembling the rustls pair should succeed");
  assert!(
    !client.enable_early_data,
    "the managed build path must force client `enable_early_data = false`"
  );
  assert_eq!(
    server.max_early_data_size, 0,
    "the managed build path must force server `max_early_data_size = 0`"
  );
}

/// Load-bearing mutation anchor for the force-off. rustls already DEFAULTS early
/// data off, so the introspection test above stays green even if a force-off
/// write is deleted. Here we DELIBERATELY PRE-ENABLE early data on both configs
/// (mirroring the caller escape hatch that opts in) and run them THROUGH
/// [`force_early_data_off`] — so deleting either assignment leaves the pre-enabled
/// value in place and fails an assertion below.
#[test]
fn force_early_data_off_disables_pre_enabled_early_data() {
  install_provider();
  let dir = unique_dir();
  let (cert, key, ca) = write_self_signed(&dir);

  let provider = rustls::crypto::CryptoProvider::get_default()
    .cloned()
    .expect("a default CryptoProvider is installed by install_provider()");
  let certs = load_certs(&cert).unwrap();
  let priv_key = load_private_key(&key).unwrap();
  let roots = load_roots(&ca).unwrap();

  let mode = ClientAuthMode::TrustedNetwork;
  // `build_server_config` borrows; `build_client_config` consumes — so build the
  // server first, then move the material into the client.
  let mut server = build_server_config(&provider, &roots, &certs, &priv_key, mode).unwrap();
  let mut client = build_client_config(provider, roots, certs, priv_key, mode).unwrap();

  // Deliberately pre-ENABLE 0-RTT on both, the state a caller opting into early
  // data would produce.
  client.enable_early_data = true;
  server.max_early_data_size = u32::MAX;
  assert!(
    client.enable_early_data,
    "precondition: the client config must start with early data ENABLED"
  );
  assert_ne!(
    server.max_early_data_size, 0,
    "precondition: the server config must start with early data ENABLED"
  );

  // The force-off helper must disable BOTH regardless of the incoming state.
  force_early_data_off(&mut client, &mut server);
  assert!(
    !client.enable_early_data,
    "force_early_data_off must set client `enable_early_data = false`"
  );
  assert_eq!(
    server.max_early_data_size, 0,
    "force_early_data_off must set server `max_early_data_size = 0`"
  );
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

/// The DoS-prevention regression: a config file that omits ALL the new
/// admission fields must deserialize to the SAME defaults `QuicOptions::new`
/// installs — `32` / `64` / `Some(1024)` / `Some(16)` — NOT the bare-`serde`
/// defaults (`0` / `0` / `None` / `None`). A `0` prefix is a legitimate
/// whole-family bucket `validate` accepts, so a bare `#[serde(default)]` would
/// silently collapse the entire IPv4 internet into one 16-slot bucket.
#[cfg(feature = "serde")]
#[test]
fn quic_config_options_serde_omitted_admission_fields_keep_safe_defaults() {
  let j = r#"{"cert_file":"/c.pem","key_file":"/k.pem","ca_file":"/ca.pem"}"#;
  let opts: QuicConfigOptions = serde_json::from_str(j).unwrap();
  assert_eq!(
    opts.pending_source_prefix_v4(),
    32,
    "omitted v4 prefix must default to /32, NOT 0 (the whole-IPv4 bucket)"
  );
  assert_eq!(
    opts.pending_source_prefix_v6(),
    64,
    "omitted v6 prefix must default to /64, NOT 0"
  );
  assert_eq!(
    opts.max_pending_inbound_total(),
    Some(DEFAULT_MAX_PENDING_INBOUND_TOTAL),
    "omitted budget must default to Some(1024), NOT None"
  );
  assert_eq!(
    opts.max_pending_connections_per_source(),
    Some(DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE),
    "omitted per-source cap must default to Some(16), NOT None"
  );
  // The matching non-zero defaults; a bucket keyed on `0.0.0.0` would be the
  // whole-internet DoS this test guards against.
  assert_eq!(DEFAULT_PENDING_SOURCE_PREFIX_V4, 32);
  assert_eq!(DEFAULT_PENDING_SOURCE_PREFIX_V6, 64);
}

/// Explicit admission fields round-trip through serde.
#[cfg(feature = "serde")]
#[test]
fn quic_config_options_serde_admission_fields_round_trip() {
  let opts = QuicConfigOptions::new(
    PathBuf::from("/c.pem"),
    PathBuf::from("/k.pem"),
    PathBuf::from("/ca.pem"),
  )
  .with_pending_source_prefix_v4(24)
  .with_pending_source_prefix_v6(48)
  .with_max_pending_inbound_total(Some(256))
  .with_max_pending_connections_per_source(None);
  let j = serde_json::to_string(&opts).unwrap();
  let back: QuicConfigOptions = serde_json::from_str(&j).unwrap();
  assert_eq!(opts, back);
  assert_eq!(back.pending_source_prefix_v4(), 24);
  assert_eq!(back.pending_source_prefix_v6(), 48);
  assert_eq!(back.max_pending_inbound_total(), Some(256));
  assert_eq!(back.max_pending_connections_per_source(), None);
}

/// `QuicConfigOptions::build` propagates the per-source cap, both prefixes, and
/// the budget into the built `QuicOptions` — the NAT escape hatch must be
/// reachable without a raw `QuicOptions`.
#[test]
fn build_propagates_admission_knobs() {
  install_provider();
  let dir = unique_dir();
  let (cert, key, ca) = write_self_signed(&dir);

  let built = QuicConfigOptions::new(cert, key, ca)
    .with_pending_source_prefix_v4(24)
    .with_pending_source_prefix_v6(48)
    .with_max_pending_connections_per_source(Some(7))
    .with_max_pending_inbound_total(Some(300))
    .build()
    .expect("build should succeed");

  assert_eq!(built.pending_source_prefix_v4(), 24);
  assert_eq!(built.pending_source_prefix_v6(), 48);
  assert_eq!(built.max_pending_connections_per_source(), Some(7));
  assert_eq!(built.max_pending_inbound_total(), Some(300));
  // And the composed prefix the connection table consumes reflects both.
  assert_eq!(built.source_prefix().v4(), 24);
  assert_eq!(built.source_prefix().v6(), 48);
  // The plumbed values still pass validation.
  built.validate().expect("propagated knobs validate");

  // The default build carries the documented defaults.
  let dir = unique_dir();
  let (cert, key, ca) = write_self_signed(&dir);
  let built = QuicConfigOptions::new(cert, key, ca)
    .build()
    .expect("default build succeeds");
  assert_eq!(built.pending_source_prefix_v4(), 32);
  assert_eq!(built.pending_source_prefix_v6(), 64);
  assert_eq!(
    built.max_pending_inbound_total(),
    Some(DEFAULT_MAX_PENDING_INBOUND_TOTAL)
  );
  assert_eq!(
    built.max_pending_connections_per_source(),
    Some(DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE)
  );
}

/// The clap mirror carries the new admission args (fresh parse yields the
/// defaults; a supplied override is applied) with distinct env ids.
#[cfg(feature = "clap")]
#[test]
fn quic_config_options_clap_admission_args() {
  use clap::{CommandFactory, Parser};

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    quic: QuicConfigOptions,
  }

  // Fresh parse with only the required paths yields the admission defaults.
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
  assert_eq!(cli.quic.pending_source_prefix_v4(), 32);
  assert_eq!(cli.quic.pending_source_prefix_v6(), 64);
  assert_eq!(
    cli.quic.max_pending_inbound_total(),
    Some(DEFAULT_MAX_PENDING_INBOUND_TOTAL)
  );
  assert_eq!(
    cli.quic.max_pending_connections_per_source(),
    Some(DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE)
  );

  // Supplied overrides are applied.
  let cli = Cli::try_parse_from([
    "prog",
    "--quic-cert",
    "/c",
    "--quic-key",
    "/k",
    "--quic-ca",
    "/ca",
    "--quic-pending-source-prefix-v4",
    "24",
    "--quic-pending-source-prefix-v6",
    "48",
    "--quic-max-pending-per-source",
    "9",
    "--quic-max-pending-inbound-total",
    "512",
  ])
  .unwrap();
  assert_eq!(cli.quic.pending_source_prefix_v4(), 24);
  assert_eq!(cli.quic.pending_source_prefix_v6(), 48);
  assert_eq!(cli.quic.max_pending_connections_per_source(), Some(9));
  assert_eq!(cli.quic.max_pending_inbound_total(), Some(512));

  let cmd = Cli::command();
  let env_vars: Vec<_> = cmd
    .get_arguments()
    .filter_map(|a| a.get_env().and_then(|e| e.to_str()))
    .collect();
  for v in [
    "MEMBERLIST_QUIC_MAX_PENDING_PER_SOURCE",
    "MEMBERLIST_QUIC_PENDING_SOURCE_PREFIX_V4",
    "MEMBERLIST_QUIC_PENDING_SOURCE_PREFIX_V6",
    "MEMBERLIST_QUIC_MAX_PENDING_INBOUND_TOTAL",
  ] {
    assert!(env_vars.contains(&v), "missing env var {v}");
  }
}
