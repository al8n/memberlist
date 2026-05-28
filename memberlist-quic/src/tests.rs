#![allow(missing_docs, warnings)]

use core::panic;
use std::{future::Future, net::SocketAddr, sync::Arc};

use agnostic_lite::RuntimeLite;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{FutureExt, Stream, lock::Mutex};
use memberlist_core::{
  proto::{Label, Message},
  tests::AnyError,
  transport::Transport,
};
use nodecraft::CheapClone;
use smol_str::SmolStr;

use crate::{QuicAcceptor, QuicConnection, QuicConnector, QuicStream, StreamLayer};

#[cfg(feature = "quinn")]
pub use quinn_stream_layer::*;

#[cfg(feature = "quinn")]
mod quinn_stream_layer {
  use super::*;
  use crate::stream_layer::quinn::*;
  use ::quinn::{ClientConfig, Endpoint, ServerConfig};
  use futures::Future;
  use quinn::{EndpointConfig, crypto::rustls::QuicClientConfig};
  use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
  use smol_str::SmolStr;
  use std::{
    error::Error,
    net::SocketAddr,
    sync::{
      Arc,
      atomic::{AtomicU16, Ordering},
    },
    time::Duration,
  };

  /// Dummy certificate verifier that treats any certificate as valid.
  /// NOTE, such verification is vulnerable to MITM attacks, but convenient for testing.
  #[derive(Debug)]
  struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

  impl SkipServerVerification {
    fn new() -> Arc<Self> {
      Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
  }

  impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
      &self,
      _end_entity: &CertificateDer<'_>,
      _intermediates: &[CertificateDer<'_>],
      _server_name: &ServerName<'_>,
      _ocsp: &[u8],
      _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
      Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
      &self,
      message: &[u8],
      cert: &CertificateDer<'_>,
      dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      rustls::crypto::verify_tls12_signature(
        message,
        cert,
        dss,
        &self.0.signature_verification_algorithms,
      )
    }

    fn verify_tls13_signature(
      &self,
      message: &[u8],
      cert: &CertificateDer<'_>,
      dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      rustls::crypto::verify_tls13_signature(
        message,
        cert,
        dss,
        &self.0.signature_verification_algorithms,
      )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
      self.0.signature_verification_algorithms.supported_schemes()
    }
  }

  fn configures() -> Result<(ServerConfig, ClientConfig), Box<dyn Error + Send + Sync + 'static>> {
    let (server_config, _) = configure_server()?;
    let client_config = configure_client();
    Ok((server_config, client_config))
  }

  fn configure_client() -> ClientConfig {
    ClientConfig::new(Arc::new(
      QuicClientConfig::try_from(
        rustls::ClientConfig::builder()
          .dangerous()
          .with_custom_certificate_verifier(SkipServerVerification::new())
          .with_no_client_auth(),
      )
      .unwrap(),
    ))
  }

  fn configure_server()
  -> Result<(ServerConfig, CertificateDer<'static>), Box<dyn Error + Send + Sync + 'static>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = CertificateDer::from(cert.cert);
    let priv_key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

    let mut server_config =
      ServerConfig::with_single_cert(vec![cert_der.clone()], priv_key.into())?;
    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    transport_config.max_concurrent_uni_streams(0_u8.into());

    Ok((server_config, cert_der))
  }

  #[allow(unused)]
  const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

  /// Returns a new quinn stream layer
  pub async fn quinn_stream_layer<R: RuntimeLite>() -> crate::quinn::Options {
    let server_name = "localhost".to_string();
    let (server_config, client_config) = configures().unwrap();
    Options::new(
      server_name,
      server_config,
      client_config,
      EndpointConfig::default(),
    )
  }

  /// Returns a new quinn stream layer
  pub async fn quinn_stream_layer_with_connect_timeout<R: RuntimeLite>(
    timeout: Duration,
  ) -> crate::quinn::Options {
    let server_name = "localhost".to_string();
    let (server_config, client_config) = configures().unwrap();
    Options::new(
      server_name,
      server_config,
      client_config,
      EndpointConfig::default(),
    )
    .with_connect_timeout(timeout)
  }
}

#[cfg(all(test, feature = "tokio", feature = "quinn"))]
mod unit_tests {
  use std::{borrow::Cow, io, net::SocketAddr, time::Duration};

  use agnostic_lite::{RuntimeLite, tokio::TokioRuntime};
  use bytes::Bytes;
  use memberlist_core::{
    proto::{CIDRsPolicy, Payload, ProtoWriter as _},
    transport::{Connection as _, Transport as _, TransportError as _},
  };
  use nodecraft::resolver::socket_addr::SocketAddrResolver;
  use smol_str::SmolStr;

  use crate::{
    QuicAcceptor as _, QuicConnection as _, QuicConnector as _, QuicStream as _, QuicTransport,
    QuicTransportError, QuicTransportOptions, StreamLayer as _, stream_layer::quinn::Quinn,
  };

  type TestResolver = SocketAddrResolver<TokioRuntime>;
  type TestLayer = Quinn<TokioRuntime>;
  type TestTransport = QuicTransport<SmolStr, TestResolver, TestLayer, TokioRuntime>;
  type DefaultOptionsTransport = QuicTransport<SmolStr, TestResolver, NoopLayer, TokioRuntime>;

  struct NoopLayer;
  struct NoopAcceptor;
  struct NoopConnector;
  #[derive(Clone)]
  struct NoopConn;
  struct NoopStream;

  impl memberlist_core::transport::Connection for NoopStream {
    type Reader = peekable::future::AsyncPeekable<futures::io::Cursor<Vec<u8>>>;
    type Writer = futures::io::Cursor<Vec<u8>>;

    fn split(self) -> (Self::Reader, Self::Writer) {
      (
        peekable::future::AsyncPeekable::from(futures::io::Cursor::new(Vec::new())),
        futures::io::Cursor::new(Vec::new()),
      )
    }

    async fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
      Ok(0)
    }

    async fn read_exact(&mut self, _buf: &mut [u8]) -> io::Result<()> {
      Ok(())
    }

    async fn peek(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
      Ok(0)
    }

    async fn peek_exact(&mut self, _buf: &mut [u8]) -> io::Result<()> {
      Ok(())
    }

    async fn write_all(&mut self, _payload: &[u8]) -> io::Result<()> {
      Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
      Ok(())
    }

    async fn close(&mut self) -> io::Result<()> {
      Ok(())
    }
  }

  impl crate::QuicStream for NoopStream {
    type SendStream = futures::io::Cursor<Vec<u8>>;

    async fn read_packet(&mut self) -> io::Result<Bytes> {
      Ok(Bytes::new())
    }
  }

  impl crate::QuicConnection for NoopConn {
    type Stream = NoopStream;

    async fn accept_bi(&self) -> io::Result<(Self::Stream, SocketAddr)> {
      Ok((NoopStream, loopback(0)))
    }

    async fn open_bi(&self) -> io::Result<(Self::Stream, SocketAddr)> {
      Ok((NoopStream, loopback(0)))
    }

    async fn send_datagram(&self, _data: Bytes) -> io::Result<()> {
      Ok(())
    }

    async fn recv_datagram(&self) -> io::Result<Bytes> {
      Ok(Bytes::new())
    }

    async fn close(&self) -> io::Result<()> {
      Ok(())
    }

    async fn is_closed(&self) -> bool {
      false
    }

    fn local_addr(&self) -> SocketAddr {
      loopback(0)
    }
  }

  impl crate::QuicAcceptor for NoopAcceptor {
    type Connection = NoopConn;

    async fn accept(&mut self) -> io::Result<(Self::Connection, SocketAddr)> {
      Ok((NoopConn, loopback(0)))
    }

    async fn close(&mut self) -> io::Result<()> {
      Ok(())
    }

    fn local_addr(&self) -> SocketAddr {
      loopback(0)
    }
  }

  impl crate::QuicConnector for NoopConnector {
    type Connection = NoopConn;

    async fn connect(&self, _addr: SocketAddr) -> io::Result<Self::Connection> {
      Ok(NoopConn)
    }

    async fn close(&self) -> io::Result<()> {
      Ok(())
    }

    async fn wait_idle(&self) -> io::Result<()> {
      Ok(())
    }

    fn local_addr(&self) -> SocketAddr {
      loopback(0)
    }
  }

  impl crate::StreamLayer for NoopLayer {
    type Runtime = TokioRuntime;
    type Acceptor = NoopAcceptor;
    type Connector = NoopConnector;
    type Stream = NoopStream;
    type Connection = NoopConn;
    type Options = ();

    fn max_stream_data(&self) -> usize {
      usize::MAX
    }

    async fn new(_options: Self::Options) -> io::Result<Self> {
      Ok(Self)
    }

    async fn bind(
      &self,
      addr: SocketAddr,
    ) -> io::Result<(SocketAddr, Self::Acceptor, Self::Connector)> {
      Ok((addr, NoopAcceptor, NoopConnector))
    }
  }

  fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .unwrap()
  }

  fn payload(bytes: &[u8]) -> Payload {
    let mut payload = Payload::new(0, bytes.len());
    payload.data_mut().copy_from_slice(bytes);
    payload
  }

  fn payload_with_header(header: &[u8], data: &[u8]) -> Payload {
    let mut payload = Payload::new(header.len(), data.len());
    payload.header_mut().copy_from_slice(header);
    payload.data_mut().copy_from_slice(data);
    payload
  }

  fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
  }

  async fn transport(id: &str) -> TestTransport {
    transport_with(id, None, None, loopback(0)).await
  }

  async fn transport_with(
    id: &str,
    timeout: Option<Duration>,
    advertise: Option<SocketAddr>,
    bind_addr: SocketAddr,
  ) -> TestTransport {
    let mut opts =
      QuicTransportOptions::<SmolStr, TestResolver, TestLayer>::with_stream_layer_options(
        id.into(),
        super::quinn_stream_layer::<TokioRuntime>().await,
      );
    opts.add_bind_address(bind_addr);
    if let Some(timeout) = timeout {
      opts = opts.with_timeout(Some(timeout));
    }
    if let Some(advertise) = advertise {
      opts = opts.with_advertise_address(advertise);
    }
    TestTransport::new(opts).await.unwrap()
  }

  fn unused_loopback_addr() -> SocketAddr {
    let sock = std::net::UdpSocket::bind(loopback(0)).unwrap();
    sock.local_addr().unwrap()
  }

  #[test]
  fn options_defaults_clone_and_into_parts() {
    let _default_opts =
      <DefaultOptionsTransport as memberlist_core::transport::Transport>::Options::new(
        "node-default".into(),
      );
    let _resolver_opts =
      <DefaultOptionsTransport as memberlist_core::transport::Transport>::Options::with_resolver_options(
        "node-resolver".into(),
        (),
      );

    let opts =
      QuicTransportOptions::<SmolStr, TestResolver, TestLayer>::with_resolver_options_and_stream_layer_options(
        "node-a".into(),
        (),
        rt().block_on(super::quinn_stream_layer::<TokioRuntime>()),
      )
      .with_timeout(Some(Duration::from_millis(250)))
      .with_connection_ttl(Some(Duration::from_millis(500)))
      .with_connection_pool_cleanup_period(Duration::from_millis(750))
      .with_max_packet_size(512)
      .with_cidrs_policy(CIDRsPolicy::block_all())
      .with_advertise_address(loopback(9000));

    let mut cloned = opts.clone();
    cloned.add_bind_address(loopback(0));
    cloned.add_bind_address(loopback(0));

    assert_eq!(cloned.id(), "node-a");
    assert_eq!(cloned.bind_addresses().len(), 1);
    assert_eq!(cloned.advertise_address(), Some(&loopback(9000)));
    assert_eq!(cloned.timeout(), Some(Duration::from_millis(250)));
    assert_eq!(cloned.connection_ttl(), Some(Duration::from_millis(500)));
    assert_eq!(
      cloned.connection_pool_cleanup_period(),
      Duration::from_millis(750)
    );
    assert_eq!(cloned.max_packet_size(), 512);
    assert!(cloned.cidrs_policy().is_block_all());
  }

  #[test]
  fn quinn_options_defaults_and_stream_data_limit() {
    rt().block_on(async {
      let opts = super::quinn_stream_layer::<TokioRuntime>().await;
      assert_eq!(opts.server_name(), "localhost");
      assert_eq!(*opts.connect_timeout(), Duration::from_secs(10));
      assert_eq!(
        *opts.max_idle_timeout(),
        Duration::from_secs(10).as_millis() as u32
      );
      assert_eq!(*opts.keep_alive_interval(), Duration::from_secs(8));
      assert_eq!(*opts.max_concurrent_stream_limit(), 256);
      assert_eq!(*opts.max_stream_data(), 10_000_000);
      assert_eq!(*opts.max_connection_data(), 15_000_000);
      assert_eq!(*opts.max_packet_size(), 10_000_000);

      let layer = TestLayer::new(
        opts
          .clone()
          .with_max_stream_data(64)
          .with_max_connection_data(32)
          .with_max_packet_size(128),
      )
      .await
      .unwrap();
      assert_eq!(layer.max_stream_data(), 32);
    });
  }

  #[test]
  fn transport_error_classifies_remote_failures() {
    let remote_kinds = [
      io::ErrorKind::ConnectionRefused,
      io::ErrorKind::ConnectionReset,
      io::ErrorKind::ConnectionAborted,
      io::ErrorKind::BrokenPipe,
      io::ErrorKind::TimedOut,
      io::ErrorKind::NotConnected,
    ];

    for kind in remote_kinds {
      let err = QuicTransportError::<TestResolver>::Io(io::Error::from(kind));
      assert!(err.is_remote_failure(), "{kind:?}");
    }

    assert!(!QuicTransportError::<TestResolver>::PacketTooLarge(1024).is_remote_failure());
    assert!(!QuicTransportError::<TestResolver>::NoPrivateIP.is_remote_failure());
    assert!(!QuicTransportError::<TestResolver>::EmptyBindAddresses.is_remote_failure());
    assert!(
      !QuicTransportError::<TestResolver>::Custom(Cow::Borrowed("custom")).is_remote_failure()
    );
    assert!(matches!(
      QuicTransportError::<TestResolver>::custom(Cow::Borrowed("via-trait")),
      QuicTransportError::Custom(Cow::Borrowed("via-trait"))
    ));
  }

  #[test]
  fn advertise_address_index_prefers_specific_addresses() {
    let unspecified_v4 = SocketAddr::from(([0, 0, 0, 0], 1));
    let specific_v4 = loopback(2);
    let unspecified_v6 = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 3));
    let specific_v6 = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 4));

    assert_eq!(
      TestTransport::find_advertise_addr_index(&[unspecified_v4, specific_v4]),
      1
    );
    assert_eq!(
      TestTransport::find_advertise_addr_index(&[unspecified_v4, unspecified_v6]),
      0
    );
    assert_eq!(
      TestTransport::find_advertise_addr_index(&[unspecified_v4, specific_v6]),
      1
    );
  }

  #[test]
  fn empty_bind_addresses_are_rejected() {
    rt().block_on(async {
      let opts =
        QuicTransportOptions::<SmolStr, TestResolver, TestLayer>::with_stream_layer_options(
          "empty".into(),
          super::quinn_stream_layer::<TokioRuntime>().await,
        );

      let err = match TestTransport::new(opts).await {
        Ok(_) => panic!("transport creation should fail without bind addresses"),
        Err(err) => err,
      };
      assert!(matches!(err, QuicTransportError::EmptyBindAddresses));
    });
  }

  #[test]
  fn non_zero_bind_and_advertise_address_are_used() {
    rt().block_on(async {
      let advertise = loopback(9191);
      let bind_addr = unused_loopback_addr();
      let transport = transport_with("non-zero", None, Some(advertise), bind_addr).await;

      assert_eq!(*transport.advertise_address(), advertise);
      assert_eq!(*transport.local_address(), bind_addr);
      assert_eq!(
        transport.resolve(transport.local_address()).await.unwrap(),
        bind_addr
      );
      assert!(transport.blocked_address(&bind_addr).is_ok());

      transport.shutdown().await.unwrap();
    });
  }

  #[test]
  fn new_transport_exposes_local_metadata_and_blocks_disallowed_ips() {
    rt().block_on(async {
      let mut opts =
        QuicTransportOptions::<SmolStr, TestResolver, TestLayer>::with_stream_layer_options(
          "metadata".into(),
          super::quinn_stream_layer::<TokioRuntime>().await,
        )
        .with_max_packet_size(1024)
        .with_cidrs_policy(CIDRsPolicy::block_all());
      opts.add_bind_address(loopback(0));
      let transport = TestTransport::new(opts).await.unwrap();

      assert_eq!(transport.local_id(), "metadata");
      assert_eq!(transport.header_overhead(), 0);
      assert!(!transport.packet_reliable());
      assert!(transport.packet_secure());
      assert!(transport.stream_secure());
      assert!(transport.max_packet_size() <= 1024);
      assert_eq!(transport.local_address().ip(), loopback(0).ip());
      assert_eq!(transport.advertise_address().ip(), loopback(0).ip());
      assert!(matches!(
        transport.blocked_address(&loopback(8080)),
        Err(QuicTransportError::BlockedIp(_))
      ));

      transport.shutdown().await.unwrap();
      transport.shutdown().await.unwrap();
    });
  }

  #[test]
  fn datagrams_are_sent_to_packet_subscriber() {
    rt().block_on(async {
      let t1 = transport_with(
        "datagram-1",
        Some(Duration::from_secs(2)),
        None,
        loopback(0),
      )
      .await;
      let t2 = transport("datagram-2").await;
      let sent_at = TokioRuntime::now();

      let (len, timestamp) = t1
        .send_to(
          t2.advertise_address(),
          payload_with_header(b"ignored", b"hello"),
        )
        .await
        .unwrap();
      assert_eq!(len, 12);
      assert!(timestamp >= sent_at);

      let packet = TokioRuntime::timeout(Duration::from_secs(2), t2.packet().recv())
        .await
        .unwrap()
        .unwrap();
      let (from, _, bytes) = packet.into_components();
      assert_eq!(from, *t1.advertise_address());
      assert_eq!(bytes, Bytes::from_static(b"ignoredhello"));

      t1.shutdown().await.unwrap();
      t2.shutdown().await.unwrap();
    });
  }

  #[test]
  fn bidirectional_streams_are_delivered_to_stream_subscriber() {
    rt().block_on(async {
      let t1 = transport_with("stream-1", Some(Duration::from_secs(2)), None, loopback(0)).await;
      let t2 = transport("stream-2").await;
      let deadline = TokioRuntime::now() + Duration::from_secs(2);

      let outbound = t1.open(t2.advertise_address(), deadline).await.unwrap();
      let (_, mut writer) = outbound.split();
      assert_eq!(
        t1.write(&mut writer, payload(b"stream-data"))
          .await
          .unwrap(),
        11
      );
      writer.close().await.unwrap();

      let (from, mut inbound) = TokioRuntime::timeout(Duration::from_secs(2), t2.stream().recv())
        .await
        .unwrap()
        .unwrap();
      assert_eq!(from, *t1.advertise_address());
      assert_eq!(
        inbound.read_packet().await.unwrap(),
        Bytes::from_static(b"stream-data")
      );

      let mut outbound = t1.open(t2.advertise_address(), deadline).await.unwrap();
      outbound.write_all(b"abcdef").await.unwrap();
      outbound.flush().await.unwrap();
      outbound.close().await.unwrap();

      let (_, mut inbound) = TokioRuntime::timeout(Duration::from_secs(2), t2.stream().recv())
        .await
        .unwrap()
        .unwrap();
      let mut reader = [0; 3];
      assert_eq!(inbound.peek(&mut reader[..2]).await.unwrap(), 2);
      assert_eq!(&reader[..2], b"ab");
      inbound.peek_exact(&mut reader).await.unwrap();
      assert_eq!(&reader, b"abc");
      assert_eq!(inbound.read(&mut reader[..2]).await.unwrap(), 2);
      assert_eq!(&reader[..2], b"ab");
      let mut rest = [0; 4];
      inbound.read_exact(&mut rest).await.unwrap();
      assert_eq!(&rest, b"cdef");

      let (mut empty_reader, _) = t1
        .open(t2.advertise_address(), deadline)
        .await
        .unwrap()
        .split();
      assert_eq!(
        t1.read(t2.advertise_address(), &mut empty_reader)
          .await
          .unwrap(),
        0
      );

      t1.shutdown().await.unwrap();
      t2.shutdown().await.unwrap();
    });
  }

  #[test]
  fn connection_pool_reuses_open_connections() {
    rt().block_on(async {
      let t1 = transport("pool-1").await;
      let t2 = transport("pool-2").await;
      let addr = *t2.advertise_address();

      let first = t1.send_to(&addr, payload(b"one")).await.unwrap();
      let second = t1.send_to(&addr, payload(b"two")).await.unwrap();
      assert_eq!(first.0, 3);
      assert_eq!(second.0, 3);

      t1.shutdown().await.unwrap();
      t2.shutdown().await.unwrap();
    });
  }

  #[test]
  fn connection_pool_cleaner_removes_closed_connections() {
    rt().block_on(async {
      let t1 = transport("cleaner-1").await;
      let t2 = transport("cleaner-2").await;
      let addr = *t2.advertise_address();
      let conn = t1.fetch_connection(addr).await.unwrap();
      assert_eq!(t1.connection_pool.len(), 1);

      conn.close().await.unwrap();
      let (_shutdown_tx, shutdown_rx) = async_channel::bounded(1);
      TokioRuntime::timeout(
        Duration::from_millis(100),
        TestTransport::connection_pool_cleaner(
          t1.connection_pool.clone(),
          TokioRuntime::interval(Duration::from_millis(1)),
          shutdown_rx,
          Duration::ZERO,
        ),
      )
      .await
      .unwrap_err();
      assert_eq!(t1.connection_pool.len(), 0);

      t1.shutdown().await.unwrap();
      t2.shutdown().await.unwrap();
    });
  }

  #[test]
  fn quinn_layer_bind_acceptor_and_connector_accessors() {
    rt().block_on(async {
      let layer = TestLayer::new(super::quinn_stream_layer::<TokioRuntime>().await)
        .await
        .unwrap();
      let (local_addr, mut acceptor, connector) = layer.bind(loopback(0)).await.unwrap();
      let cloned = acceptor.clone();

      assert_eq!(acceptor.local_addr(), local_addr);
      assert_eq!(cloned.local_addr(), local_addr);
      assert_eq!(connector.local_addr(), local_addr);

      connector.close().await.unwrap();
      connector.wait_idle().await.unwrap();
      acceptor.close().await.unwrap();
    });
  }

  #[test]
  fn quinn_connection_direct_datagram_and_stream_round_trip() {
    rt().block_on(async {
      let layer1 = TestLayer::new(super::quinn_stream_layer::<TokioRuntime>().await)
        .await
        .unwrap();
      let layer2 = TestLayer::new(super::quinn_stream_layer::<TokioRuntime>().await)
        .await
        .unwrap();
      let (addr1, _acceptor1, connector1) = layer1.bind(loopback(0)).await.unwrap();
      let (addr2, mut acceptor2, _connector2) = layer2.bind(loopback(0)).await.unwrap();

      let connect = connector1.connect(addr2);
      let accept = acceptor2.accept();
      let (client_conn, accepted) = TokioRuntime::timeout(Duration::from_secs(2), async {
        futures::join!(connect, accept)
      })
      .await
      .unwrap();
      let client_conn = client_conn.unwrap();
      let (server_conn, remote_addr) = accepted.unwrap();
      assert_eq!(remote_addr, addr1);
      assert_eq!(client_conn.local_addr(), addr1);
      assert_eq!(server_conn.local_addr(), addr2);
      assert!(!client_conn.is_closed().await);

      client_conn
        .send_datagram(Bytes::from_static(b"direct-datagram"))
        .await
        .unwrap();
      assert_eq!(
        server_conn.recv_datagram().await.unwrap(),
        Bytes::from_static(b"direct-datagram")
      );

      let (mut client_stream, stream_addr) = client_conn.open_bi().await.unwrap();
      assert_eq!(stream_addr, addr2);
      client_stream.write_all(b"direct-stream").await.unwrap();
      client_stream.flush().await.unwrap();
      client_stream.close().await.unwrap();

      let (mut server_stream, stream_addr) = server_conn.accept_bi().await.unwrap();
      assert_eq!(stream_addr, addr1);
      assert_eq!(
        server_stream.read_packet().await.unwrap(),
        Bytes::from_static(b"direct-stream")
      );

      client_conn.close().await.unwrap();
      server_conn.close().await.unwrap();
      assert!(client_conn.is_closed().await);
      connector1.close().await.unwrap();
      acceptor2.close().await.unwrap();
    });
  }
}
