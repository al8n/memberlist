use super::*;

macro_rules! invoke_ack_handler_channel_ack {
  ($rt: ident) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _invoke_ack_handler_channel_ack >]() {
        [< $rt:snake _run >](async move {
          invoke_ack_handler_channel_ack::<[< $rt:camel Runtime >]>().await;
        });
      }
    }
  };
}

#[cfg(feature = "tokio")]
mod tokio {
  use agnostic::tokio::TokioRuntime;

  use super::*;

  invoke_ack_handler_channel_ack!(tokio);
}

#[cfg(feature = "async-std")]
mod async_std {

  use super::*;

  invoke_ack_handler_channel_ack!(async_std);
}

#[cfg(feature = "smol")]
mod smol {

  use super::*;

  invoke_ack_handler_channel_ack!(smol);
}
