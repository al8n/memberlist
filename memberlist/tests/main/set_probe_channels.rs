use super::*;

macro_rules! set_probe_channels {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _set_probe_channels >]() {
        [< $rt:snake _run >](async move {
          set_probe_channels::<[< $rt:camel Runtime >]>().await;
        });
      }
    }
  };
}

#[cfg(feature = "tokio")]
mod tokio {
  use agnostic::tokio::TokioRuntime;

  use super::*;

  set_probe_channels!(tokio("tcp", Tcp::<TokioRuntime>::new()));

  #[cfg(feature = "tls")]
  set_probe_channels!(tokio(
    "tls",
    memberlist_net::tests::tls_stream_layer::<TokioRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  set_probe_channels!(tokio(
    "native-tls",
    memberlist_net::tests::native_tls_stream_layer::<TokioRuntime>().await
  ));
}

#[cfg(feature = "async-std")]
mod async_std {

  use super::*;

  set_probe_channels!(async_std("tcp", Tcp::<AsyncStdRuntime>::new()));

  #[cfg(feature = "tls")]
  set_probe_channels!(async_std(
    "tls",
    memberlist_net::tests::tls_stream_layer::<AsyncStdRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  set_probe_channels!(async_std(
    "native-tls",
    memberlist_net::tests::native_tls_stream_layer::<AsyncStdRuntime>().await
  ));
}

#[cfg(feature = "smol")]
mod smol {

  use super::*;

  set_probe_channels!(smol("tcp", Tcp::<SmolRuntime>::new()));

  #[cfg(feature = "tls")]
  set_probe_channels!(smol(
    "tls",
    memberlist_net::tests::tls_stream_layer::<SmolRuntime>().await
  ));

  #[cfg(feature = "native-tls")]
  set_probe_channels!(smol(
    "native-tls",
    memberlist_net::tests::native_tls_stream_layer::<SmolRuntime>().await
  ));
}
