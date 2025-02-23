use memberlist_core::{
  Options,
  tests::{memberlist::*, next_socket_addr_v4, state::*},
  transport::{Node, Transport, resolver::socket_addr::SocketAddrResolver},
};
use memberlist_quic::{QuicTransport, QuicTransportOptions};
use smol_str::SmolStr;

macro_rules! test_mods {
  ($fn:ident) => {
    #[cfg(all(feature = "tokio", feature = "quinn"))]
    mod tokio {
      use agnostic::tokio::TokioRuntime;
      #[cfg(feature = "quinn")]
      use memberlist_quic::{tests::quinn_stream_layer, stream_layer::quinn::Quinn};

      use super::*;
      use crate::tokio_run;



      #[cfg(feature = "quinn")]
      $fn!(Quinn<tokio>("quinn", quinn_stream_layer::<TokioRuntime>().await));
    }

    #[cfg(all(feature = "async-std", feature = "quinn"))]
    mod async_std {
      use agnostic::async_std::AsyncStdRuntime;
      use memberlist_quic::{tests::quinn_stream_layer, stream_layer::quinn::Quinn};

      use super::*;
      use crate::async_std_run;

      $fn!(Quinn<async_std>(
        "quinn",
        quinn_stream_layer::<AsyncStdRuntime>().await
      ));
    }

    #[cfg(all(feature = "smol", feature = "quinn"))]
    mod smol {
      use agnostic::smol::SmolRuntime;
      use memberlist_quic::{tests::quinn_stream_layer, stream_layer::quinn::Quinn};

      use super::*;
      use crate::smol_run;

      $fn!(Quinn<smol>("quinn", quinn_stream_layer::<SmolRuntime>().await));
    }
  };
  ($fn:ident($expr:expr)) => {
    #[cfg(all(feature = "tokio", feature = "quinn"))]
    mod tokio {
      use agnostic::tokio::TokioRuntime;
      #[cfg(feature = "quinn")]
      use memberlist_quic::{tests::quinn_stream_layer_with_connect_timeout, stream_layer::quinn::Quinn};

      use super::*;
      use crate::tokio_run;

      #[cfg(feature = "quinn")]
      $fn!(Quinn<tokio>(
        "quinn",
        quinn_stream_layer_with_connect_timeout::<TokioRuntime>($expr).await
      ));
    }

    #[cfg(all(feature = "async-std", feature = "quinn"))]
    mod async_std {
      use agnostic::async_std::AsyncStdRuntime;
      use memberlist_quic::{tests::quinn_stream_layer_with_connect_timeout, stream_layer::quinn::Quinn};

      use super::*;
      use crate::async_std_run;

      $fn!(Quinn<async_std>(
        "quinn",
        quinn_stream_layer_with_connect_timeout::<AsyncStdRuntime>($expr).await
      ));
    }

    #[cfg(all(feature = "smol", feature = "quinn"))]
    mod smol {
      use agnostic::smol::SmolRuntime;
      use memberlist_quic::{tests::quinn_stream_layer_with_connect_timeout, stream_layer::quinn::Quinn};

      use super::*;
      use crate::smol_run;

      $fn!(Quinn<smol>(
        "quinn",
        quinn_stream_layer_with_connect_timeout::<SmolRuntime>($expr).await
      ));
    }
  };
}

#[path = "quic/probe.rs"]
mod probe;

#[path = "quic/probe_node.rs"]
mod probe_node;

#[cfg(not(tarpaulin))]
#[path = "quic/probe_node_dogpile.rs"]
mod probe_node_dogpile;

#[path = "quic/probe_node_buddy.rs"]
mod probe_node_buddy;

#[path = "quic/probe_node_suspect.rs"]
mod probe_node_suspect;

#[path = "quic/probe_node_awareness_missed_nack.rs"]
#[cfg(not(windows))] // TODO: I do not have a windows machine to test and fix this, need helps
mod probe_node_awareness_missed_nack;

#[path = "quic/probe_node_awareness_improved.rs"]
mod probe_node_awareness_improved;

#[path = "quic/probe_node_awareness_degraded.rs"]
mod probe_node_awareness_degraded;

#[path = "quic/ping.rs"]
mod ping;

#[path = "quic/reset_nodes.rs"]
mod reset_nodes;

#[path = "quic/alive_node_new_node.rs"]
mod alive_node_new_node;

#[path = "quic/alive_node_conflict.rs"]
mod alive_node_conflict;

#[path = "quic/alive_node_suspect_node.rs"]
mod alive_node_suspect_node;

#[path = "quic/alive_node_idempotent.rs"]
mod alive_node_idempotent;

#[path = "quic/alive_node_change_meta.rs"]
mod alive_node_change_meta;

#[path = "quic/alive_node_refute.rs"]
mod alive_node_refute;

#[path = "quic/suspect_node_no_node.rs"]
mod suspect_node_no_node;

#[path = "quic/suspect_node.rs"]
mod suspect_node;

#[path = "quic/suspect_node_double_suspect.rs"]
mod suspect_node_double_suspect;

#[path = "quic/suspect_node_refute.rs"]
mod suspect_node_refute;

#[path = "quic/dead_node_no_node.rs"]
mod dead_node_no_node;

#[path = "quic/dead_node_left.rs"]
mod dead_node_left;

#[path = "quic/dead_node.rs"]
mod dead_node;

#[path = "quic/dead_node_double.rs"]
mod dead_node_double;

#[path = "quic/dead_node_old_dead.rs"]
mod dead_node_old_dead;

#[path = "quic/dead_node_alive_replay.rs"]
mod dead_node_alive_replay;

#[path = "quic/dead_node_refute.rs"]
mod dead_node_refute;

#[path = "quic/merge_state.rs"]
mod merge_state;

#[path = "quic/gossip.rs"]
mod gossip;

#[path = "quic/gossip_to_dead.rs"]
mod gossip_to_dead;

#[path = "quic/push_pull.rs"]
mod push_pull;

// ------- memberlist tests ----------

#[path = "quic/join.rs"]
mod join;

#[path = "quic/join_with_labels.rs"]
mod join_with_labels;

#[path = "quic/join_with_labels_and_compression.rs"]
#[cfg(any(
  feature = "snappy",
  feature = "brotli",
  feature = "zstd",
  feature = "lz4",
))]
mod join_with_labels_and_compression;

#[path = "quic/join_different_networks_unique_mask.rs"]
mod join_different_networks_unique_mask;

#[path = "quic/join_different_networks_multi_masks.rs"]
#[cfg(not(windows))] // TODO: I do not have a windows machine to test and fix this, need helps
mod join_different_networks_multi_masks;

#[path = "quic/join_cancel.rs"]
mod join_cancel;

#[path = "quic/join_cancel_passive.rs"]
mod join_cancel_passive;

#[path = "quic/join_shutdown.rs"]
mod join_shutdown;

#[cfg(not(tarpaulin))]
#[path = "quic/leave.rs"]
mod leave;

#[path = "quic/conflict_delegate.rs"]
mod conflict_delegate;

#[path = "quic/create_shutdown.rs"]
mod create_shutdown;

#[path = "quic/create.rs"]
mod create;

#[path = "quic/send.rs"]
mod send;

#[path = "quic/user_data.rs"]
mod user_data;

#[path = "quic/node_delegate_meta.rs"]
mod node_delegate_meta;

#[path = "quic/node_delegate_meta_update.rs"]
mod node_delegate_meta_update;

#[path = "quic/ping_delegate.rs"]
mod ping_delegate;

#[path = "quic/shutdown_cleanup.rs"]
mod shutdown_cleanup;

#[path = "quic/send_reliable.rs"]
mod send_reliable;
