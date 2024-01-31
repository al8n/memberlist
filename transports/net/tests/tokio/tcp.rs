use std::net::SocketAddr;

use memberlist_core::{transport::Lpe, types::Message};
use memberlist_net::{stream_layer::tcp::Tcp, resolver::socket_addr::SocketAddrResolver, test::*, NetTransportOptions};
use nodecraft::Transformable;
use smol_str::SmolStr;
use super::*;


unit_tests_with_expr!(run(
  handle_v4_ping_no_label_no_compression_no_encryption ({
    let mut opts = NetTransportOptions::new("test_handle_v4_ping_no_label_no_compression_no_encryption".into());
    opts.add_bind_address(next_socket_addr_v4());
    handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |src| {
      // skip checksum
      match Message::<SmolStr, SocketAddr>::decode(&src[5..]) {
        Ok((_, msg)) => msg.unwrap_ack(),
        Err(e) => {
          tracing::error!(err=%e, src=?src, "fail to decode ack response");
          panic!("{e}")
        }
      }
    }, next_socket_addr_v4()).await;
  }),
  handle_v6_ping_no_label_no_compression_no_encryption ({
    let mut opts = NetTransportOptions::new("test_handle_v6_ping_no_label_no_compression_no_encryption".into());
    opts.add_bind_address(next_socket_addr_v6());
    handle_ping::<_, _, _, _, _, Lpe<_, _>>(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |src| {
      // skip checksum
      match Message::<SmolStr, SocketAddr>::decode(&src[5..]) {
        Ok((_, msg)) => msg.unwrap_ack(),
        Err(e) => {
          tracing::error!(err=%e, src=?src, "fail to decode ack response");
          panic!("{e}")
        }
      }
    }, next_socket_addr_v6()).await;
  }),
));
