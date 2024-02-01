use std::net::SocketAddr;

use super::*;
use memberlist_core::{transport::Lpe, types::Message};
use memberlist_net::{
  compressor::Compressor,
  resolver::socket_addr::SocketAddrResolver,
  security::{EncryptionAlgo, SecretKey},
  stream_layer::tcp::Tcp,
  test::*,
  NetTransportOptions,
};
use memberlist_utils::Label;
use nodecraft::{CheapClone, Transformable};
use smol_str::SmolStr;

// Server: powerset & Client: plain
unit_tests_with_expr!(run(
  handle_v4_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption".into());
    opts.add_bind_address(next_socket_addr_v4());
    let res = handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |src| {
      verify_checksum(src)?;
      Message::<SmolStr, SocketAddr>::decode(&src[5..]).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v4()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
  handle_v6_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption".into());
    opts.add_bind_address(next_socket_addr_v6());
    let res = handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |src| {
      verify_checksum(src)?;
      Message::<SmolStr, SocketAddr>::decode(&src[5..]).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v6()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
  handle_v4_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let label = Label::from_static("test_handle_v4_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption");
    let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption".into())
      .with_label(label.cheap_clone())
      .with_skip_inbound_label_check(true);
    opts.add_bind_address(next_socket_addr_v4());
    let res = handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |mut src| {
      let received_label = read_label(src)?;
      assert_eq!(received_label, label);
      src = &src[label.encoded_overhead()..];
      verify_checksum(src)?;
      Message::<SmolStr, SocketAddr>::decode(&src[5..]).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v4()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
  handle_v6_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let label = Label::from_static("test_handle_v6_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption");
    let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption".into())
      .with_label(label.cheap_clone())
      .with_skip_inbound_label_check(true);
    opts.add_bind_address(next_socket_addr_v6());
    let res = handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |mut src| {
      let received_label = read_label(src)?;
      assert_eq!(received_label, label);
      src = &src[label.encoded_overhead()..];
      verify_checksum(src)?;
      Message::<SmolStr, SocketAddr>::decode(&src[5..]).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v6()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),

  handle_v4_ping_server_no_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_no_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption".into()).with_compressor(Some(Compressor::Lzw));
    opts.add_bind_address(next_socket_addr_v4());
    let res = handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |src| {
      verify_checksum(src)?;
      let uncompressed = read_compressed_data(&src[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v4()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
  handle_v6_ping_server_no_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_no_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption".into()).with_compressor(Some(Compressor::Lzw));
    opts.add_bind_address(next_socket_addr_v6());
    let res = handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |src| {
      verify_checksum(src)?;
      let uncompressed = read_compressed_data(&src[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v6()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
  handle_v4_ping_server_with_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let label = Label::from_static("test_handle_v4_ping_server_with_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption");
    let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_with_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption".into())
      .with_compressor(Some(Compressor::Lzw))
      .with_label(label.cheap_clone())
      .with_skip_inbound_label_check(true);
    opts.add_bind_address(next_socket_addr_v4());
    let res = handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |mut src| {
      let received_label = read_label(src)?;
      assert_eq!(received_label, label);
      src = &src[label.encoded_overhead()..];
      verify_checksum(src)?;
      let uncompressed = read_compressed_data(&src[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v4()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
  handle_v6_ping_server_with_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let label = Label::from_static("test_handle_v6_ping_server_with_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption");
    let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_with_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption".into())
      .with_compressor(Some(Compressor::Lzw))
      .with_label(label.cheap_clone())
      .with_skip_inbound_label_check(true);
    opts.add_bind_address(next_socket_addr_v6());
    let res = handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |mut src| {
      let received_label = read_label(src)?;
      assert_eq!(received_label, label);
      src = &src[label.encoded_overhead()..];
      let uncompressed = read_compressed_data(&src[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v6()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),

  handle_v4_ping_server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption ({
    let pk = SecretKey::from([1; 32]);
    let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_no_label_no_compression_with_encryption_client_no_label_no_compression_no_encryption".into())
      .with_primary_key(Some(pk))
      .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
      .with_gossip_verify_outgoing(true)
      .with_compressor(Some(Compressor::Lzw));
    opts.add_bind_address(next_socket_addr_v4());
    let res = handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |src| {
      let unencrypted = read_encrypted_data(pk, &[], src)?;
      verify_checksum(&unencrypted)?;
      let uncompressed = read_compressed_data(&unencrypted[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v4()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
  handle_v6_ping_server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption ({
    let pk = SecretKey::from([1; 32]);
    let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_no_label_no_compression_with_encryption_client_no_label_no_compression_no_encryption".into())
      .with_primary_key(Some(pk))
      .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
      .with_gossip_verify_outgoing(true)
      .with_compressor(Some(Compressor::Lzw));
    opts.add_bind_address(next_socket_addr_v6());
    let res = handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |src| {
      let unencrypted = read_encrypted_data(pk, &[], src)?;
      verify_checksum(&unencrypted)?;
      let uncompressed = read_compressed_data(&unencrypted[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v6()).await;
    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
  handle_v4_ping_server_with_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption ({
    let label = Label::from_static("test_handle_v4_ping_server_with_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption");
    let pk = SecretKey::from([1; 32]);
    let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_with_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption".into())
      .with_primary_key(Some(pk))
      .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
      .with_gossip_verify_outgoing(true)
      .with_compressor(Some(Compressor::Lzw))
      .with_label(label.cheap_clone())
      .with_skip_inbound_label_check(true);
    opts.add_bind_address(next_socket_addr_v4());
    let res = handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |mut src| {
      let received_label = read_label(src)?;
      assert_eq!(received_label, label);
      src = &src[label.encoded_overhead()..];
      let unencrypted = read_encrypted_data(pk, received_label.as_bytes(), src)?;
      verify_checksum(&unencrypted)?;
      let uncompressed = read_compressed_data(&unencrypted[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v4()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
  handle_v6_ping_server_with_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption ({
    let label = Label::from_static("test_handle_v6_ping_server_with_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption");
    let pk = SecretKey::from([1; 32]);
    let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_with_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption".into())
      .with_primary_key(Some(pk))
      .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
      .with_gossip_verify_outgoing(true)
      .with_compressor(Some(Compressor::Lzw))
      .with_label(label.cheap_clone())
      .with_skip_inbound_label_check(true);

    opts.add_bind_address(next_socket_addr_v6());
    let res = handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |mut src| {
      let received_label = read_label(src)?;
      assert_eq!(received_label, label);
      src = &src[label.encoded_overhead()..];
      let unencrypted = read_encrypted_data(pk, received_label.as_bytes(), src)?;
      verify_checksum(&unencrypted)?;
      let uncompressed = read_compressed_data(&unencrypted[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v6()).await;
    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
));

// unit_tests_with_expr!(run(
  
// ));
