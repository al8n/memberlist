use super::*;

macro_rules! invoke_ack_handler_channel_nack {
  ($rt: ident) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _invoke_ack_handler_channel_nack >]() {
        [< $rt:snake _run >](async move {
          invoke_ack_handler_channel_nack::<[< $rt:camel Runtime >]>().await;
        });
      }
    }
  };
}

#[cfg(feature = "tokio")]
mod tokio {
  use agnostic::tokio::TokioRuntime;

  use super::*;

  invoke_ack_handler_channel_nack!(tokio);
}

#[cfg(feature = "smol")]
mod smol {

  use super::*;

  invoke_ack_handler_channel_nack!(smol);
}
