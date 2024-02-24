use super::*;

macro_rules! dead_node_refute {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _net_dead_node_refute >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _>::new("dead_node_refute_1".into());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          dead_node_refute(t1, t1_opts).await;
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

  dead_node_refute!(tokio("tcp", Tcp::<TokioRuntime>::new()));

  #[cfg(feature = "tls")]
  dead_node_refute!(tokio(
    "tls",
    memberlist_net::tests::tls_stream_layer::<TokioRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  dead_node_refute!(tokio(
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

  dead_node_refute!(async_std("tcp", Tcp::<AsyncStdRuntime>::new()));

  #[cfg(feature = "tls")]
  dead_node_refute!(async_std(
    "tls",
    memberlist_net::tests::tls_stream_layer::<AsyncStdRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  dead_node_refute!(async_std(
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

  dead_node_refute!(smol("tcp", Tcp::<SmolRuntime>::new()));

  #[cfg(feature = "tls")]
  dead_node_refute!(smol(
    "tls",
    memberlist_net::tests::tls_stream_layer::<SmolRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  dead_node_refute!(smol(
    "native-tls",
    memberlist_net::tests::native_tls_stream_layer::<SmolRuntime>().await
  ));
}
