use super::*;

macro_rules! set_ack_handler {
  ($rt: ident) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _set_ack_handler >]() {
        [< $rt:snake _run >](async move {
          set_ack_handler::<[< $rt:camel Runtime >]>().await;
        });
      }
    }
  };
}

#[cfg(feature = "tokio")]
mod tokio {
  use agnostic::tokio::TokioRuntime;

  use super::*;
  use crate::tokio_run;

  set_ack_handler!(tokio);
}

#[cfg(feature = "async-std")]
mod async_std {
  use agnostic::async_std::AsyncStdRuntime;

  use super::*;
  use crate::async_std_run;

  set_ack_handler!(async_std);
}

#[cfg(feature = "smol")]
mod smol {
  use agnostic::smol::SmolRuntime;

  use super::*;
  use crate::smol_run;

  set_ack_handler!(smol);
}
