use super::*;

macro_rules! suspect_node_no_node {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _net_suspect_node_no_node >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _>::new("suspect_node_no_node_1".into());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          suspect_node_no_node(t1, t1_opts, "test".into()).await;
        });
      }
    }
  };
}

#[cfg(feature = "tokio")]
mod tokio {
  use agnostic::tokio::TokioRuntime;
  use memberlist_net::stream_layer::tcp::Tcp;

  use super::*;
  use crate::tokio_run;

  suspect_node_no_node!(tokio("tcp", Tcp::<TokioRuntime>::new()));

  #[cfg(feature = "tls")]
  suspect_node_no_node!(tokio(
    "tls",
    memberlist_net::tests::tls_stream_layer::<TokioRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  suspect_node_no_node!(tokio(
    "native-tls",
    memberlist_net::tests::native_tls_stream_layer::<TokioRuntime>().await
  ));
}

#[cfg(feature = "async-std")]
mod async_std {
  use agnostic::async_std::AsyncStdRuntime;
  use memberlist_net::stream_layer::tcp::Tcp;

  use super::*;
  use crate::async_std_run;

  suspect_node_no_node!(async_std("tcp", Tcp::<AsyncStdRuntime>::new()));

  #[cfg(feature = "tls")]
  suspect_node_no_node!(async_std(
    "tls",
    memberlist_net::tests::tls_stream_layer::<AsyncStdRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  suspect_node_no_node!(async_std(
    "native-tls",
    memberlist_net::tests::native_tls_stream_layer::<AsyncStdRuntime>().await
  ));
}

#[cfg(feature = "smol")]
mod smol {
  use agnostic::smol::SmolRuntime;
  use memberlist_net::stream_layer::tcp::Tcp;

  use super::*;
  use crate::smol_run;

  suspect_node_no_node!(smol("tcp", Tcp::<SmolRuntime>::new()));

  #[cfg(feature = "tls")]
  suspect_node_no_node!(smol(
    "tls",
    memberlist_net::tests::tls_stream_layer::<SmolRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  suspect_node_no_node!(smol(
    "native-tls",
    memberlist_net::tests::native_tls_stream_layer::<SmolRuntime>().await
  ));
}
