use std::{
  net::SocketAddr,
  sync::atomic::{AtomicBool, AtomicU32},
};

#[cfg(feature = "async")]
use async_lock::{Mutex, RwLock};
use crossbeam_utils::CachePadded;
#[cfg(not(feature = "async"))]
use parking_lot::{Mutex, RwLock};

#[cfg(feature = "async")]
use async_channel::{Receiver, Sender};
#[cfg(not(feature = "async"))]
use crossbeam_channel::{Receiver, Sender};

struct HotData {
  sequence_num: CachePadded<AtomicU32>,
  incarnation: CachePadded<AtomicU32>,
  num_nodes: CachePadded<AtomicU32>,
  push_pull_req: CachePadded<AtomicU32>,
  shutdown: CachePadded<AtomicBool>,
  leave: CachePadded<AtomicBool>,
}

struct Advertise {
  addr: SocketAddr,
}

struct Inner {
  hot: HotData,
  advertise: RwLock<SocketAddr>,
  // Serializes calls to Shutdown
  shutdown_lock: Mutex<()>,
  // Serializes calls to Leave
  leave_lock: Mutex<()>,
}
