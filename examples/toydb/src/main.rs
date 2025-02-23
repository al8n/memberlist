use std::{borrow::Cow, net::SocketAddr, sync::Arc};

use bincode::{deserialize, serialize};
use clap::Parser;
use crossbeam_skiplist::SkipMap;
use memberlist::{
  agnostic::tokio::TokioRuntime, bytes::Bytes, delegate::{CompositeDelegate, NodeDelegate, VoidDelegate}, net::{stream_layer::tcp::Tcp, NetTransportOptions}, proto::{HostAddr, Meta, NodeId}, tokio::TokioTcpMemberlist, transport::resolver::dns::DnsResolver, Options
};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::UnixListener, sync::{mpsc::Sender, oneshot}};

type ToyDbDelegate = CompositeDelegate<
  NodeId,
  SocketAddr,
  VoidDelegate<NodeId, SocketAddr>,
  VoidDelegate<NodeId, SocketAddr>,
  VoidDelegate<NodeId, SocketAddr>,
  VoidDelegate<NodeId, SocketAddr>,
  MemDb,
>;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

struct Inner {
  meta: Meta,
  store: SkipMap<Bytes, Bytes>,
}

#[derive(Clone)]
struct MemDb {
  inner: Arc<Inner>,
}

impl NodeDelegate for MemDb {
  async fn node_meta(&self, _: usize) -> Meta {
    self.inner.meta.clone()
  }

  async fn local_state(&self, _: bool) -> Bytes {
    let all_data = self
      .inner
      .store
      .iter()
      .map(|ent| (ent.key().clone(), ent.value().clone()))
      .collect::<Vec<_>>();

    match serialize(&all_data) {
      Ok(data) => Bytes::from(data),
      Err(e) => {
        tracing::error!(err=%e, "toydb: fail to encode local state");
        Bytes::new()
      }
    }
  }

  async fn merge_remote_state(&self, buf: &[u8], _: bool) {
    match deserialize::<Vec<(Bytes, Bytes)>>(buf) {
      Ok(pairs) => {
        for (key, value) in pairs {
          self.inner.store.get_or_insert(key, value);
        }
      }
      Err(e) => {
        tracing::error!(err=%e, "toydb: fail to decode remote state");
      }
    }
  }

  async fn notify_message(&self, msg: Cow<'_, [u8]>) {
    match deserialize::<Vec<(Bytes, Bytes)>>(msg.as_ref()) {
      Ok(pairs) => {
        for (key, value) in pairs {
          self.inner.store.get_or_insert(key, value);
        }
      }
      Err(e) => {
        tracing::error!(err=%e, "toydb: fail to decode remote message");
      }
    }
  }
}

struct ToyDb {
  memberlist: TokioTcpMemberlist<NodeId, DnsResolver<TokioRuntime>, ToyDbDelegate>,
  tx: Sender<Event>,
}

impl ToyDb {
  async fn new(
    meta: Meta,
    opts: Options,
    net_opts: NetTransportOptions<NodeId, DnsResolver<TokioRuntime>, Tcp<TokioRuntime>>,
  ) -> Result<Self> {
    let memdb = MemDb {
      inner: Inner {
        meta,
        store: SkipMap::new(),
      }
      .into(),
    };

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    let memdb1 = memdb.clone();
    tokio::spawn(async move {
      tokio::select! {
        _ = tokio::signal::ctrl_c() => {
          tracing::info!("toydb: shutting down db event listener");
        }
        ev = rx.recv() => {
          if let Some(ev) = ev {
            match ev {
              Event::Get { key, tx } => {
                let value = memdb1.inner.store.get(&key).map(|ent| ent.value().clone());
                let _ = tx.send(value);
              }
              Event::Set { key, value, tx } => {
                if memdb1.inner.store.get(&key).is_some() {
                  let _ = tx.send(Err("key already exists".into()));
                } else {
                  memdb1.inner.store.insert(key, value);
                  let _ = tx.send(Ok(()));
                }
              }
              Event::Del { key, tx } => {
                let value = memdb1.inner.store.remove(&key).map(|ent| ent.value().clone());
                let _ = tx.send(value);
              }
            }
          }
        }
      }
    });

    let delegate = CompositeDelegate::<NodeId, SocketAddr>::default().with_node_delegate(memdb);
    let memberlist = TokioTcpMemberlist::with_delegate(delegate, net_opts, opts).await?;
    Ok(Self { memberlist, tx })
  }
}

#[derive(clap::Args)]
struct Command {
  /// The id of the db instance
  #[clap(short, long)]
  id: NodeId,
  /// The address the memberlist should bind to
  #[clap(short, long)]
  addr: SocketAddr,
  /// The meta data of the db instance
  #[clap(short, long)]
  meta: Meta,
  /// The rpc address to listen on commands
  #[clap(short, long, default_value = "/tmp/toydb.sock")]
  rpc_addr: std::path::PathBuf,
}

#[derive(clap::Parser)]
enum Args {
  Start(Command),
  Get {
    key: Bytes,
  },
  Set {
    key: Bytes,
    value: Bytes,
  },
  Del {
    key: Bytes,
  },
}

#[derive(serde::Serialize, serde::Deserialize)]
enum Op {
  Get(Bytes),
  Set(Bytes, Bytes),
  Del(Bytes),
}

enum Event {
  Get {
    key: Bytes,
    tx: oneshot::Sender<Option<Bytes>>,
  },
  Set {
    key: Bytes,
    value: Bytes,
    tx: oneshot::Sender<Result<()>>,
  },
  Del {
    key: Bytes,
    tx: oneshot::Sender<Option<Bytes>>,
  },
}

#[tokio::main]
async fn main() -> Result<()> {
  tracing_subscriber::fmt::init();

  let args = Args::parse();
  let opts = Options::lan();
  let net_opts = NetTransportOptions::new(
    args.id,
  ).with_bind_addresses([HostAddr::from(args.addr)].into_iter().collect());

  let db = ToyDb::new(args.meta, opts, net_opts).await?;

  let listener = UnixListener::bind(&args.rpc_addr)?;
  loop {
    tokio::select! {
      conn = listener.accept() => {
        let (stream, _) = conn?;
        let mut stream = tokio::io::BufReader::new(stream);
        let mut data = Vec::new();
        if let Err(e) = stream.read_to_end(&mut data).await {
          tracing::error!(err=%e, "toydb: fail to read from rpc stream");
          continue;
        }

        let op: Op = match bincode::deserialize(&data) {
          Ok(op) => op,
          Err(e) => {
            tracing::error!(err=%e, "toydb: fail to decode rpc message");
            continue;
          }
        };

        match op {
          Op::Get(bytes) => {
            let (tx, rx) = oneshot::channel();
            db.tx.send(Event::Get { key: bytes, tx }).await?;

            let resp = rx.await?;
            match bincode::serialize(&resp) {
              Ok(resp) => {
                if let Err(e) = stream.write_all(&resp).await {
                  tracing::error!(err=%e, "toydb: fail to write rpc response");
                }
              }
              Err(e) => {
                tracing::error!(err=%e, "toydb: fail to encode rpc response");
              }
            }
          },
          Op::Set(key, value) => {
            let (tx, rx) = oneshot::channel();
            db.tx.send(Event::Set { key, value, tx }).await?;

            let resp = rx.await?;
            if let Err(e) = resp {
              let res = std::result::Result::<(), String>::Err(e.to_string());
              match bincode::serialize(&res) {
                Ok(resp) => {
                  if let Err(e) = stream.write_all(&resp).await {
                    tracing::error!(err=%e, "toydb: fail to write rpc response");
                  }
                }
                Err(e) => {
                  tracing::error!(err=%e, "toydb: fail to encode rpc response");
                }
              }
            } else {
              let res = std::result::Result::<(), String>::Ok(());
              match bincode::serialize(&res) {
                Ok(resp) => {
                  if let Err(e) = stream.write_all(&resp).await {
                    tracing::error!(err=%e, "toydb: fail to write rpc response");
                  }
                }
                Err(e) => {
                  tracing::error!(err=%e, "toydb: fail to encode rpc response");
                }
              }
            }
          },
          Op::Del(key) => {
            let (tx, rx) = oneshot::channel();
            db.tx.send(Event::Del { key, tx }).await?;

            let resp = rx.await?;
            match bincode::serialize(&resp) {
              Ok(resp) => {
                if let Err(e) = stream.write_all(&resp).await {
                  tracing::error!(err=%e, "toydb: fail to write rpc response");
                }
              }
              Err(e) => {
                tracing::error!(err=%e, "toydb: fail to encode rpc response");
              }
            }
          },
        }
      }
      _ = tokio::signal::ctrl_c() => {
        break;
      }
    }
  }
  Ok(())
}
