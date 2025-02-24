use std::{borrow::Cow, net::SocketAddr, sync::Arc};

use bincode::{deserialize, serialize};
use clap::Parser;
use crossbeam_skiplist::SkipMap;
use memberlist::{
  Options,
  agnostic::tokio::TokioRuntime,
  bytes::Bytes,
  delegate::{CompositeDelegate, NodeDelegate},
  net::{MaybeResolvedAddress, NetTransportOptions, Node, stream_layer::tcp::Tcp},
  proto::{HostAddr, Meta, NodeId},
  tokio::TokioTcpMemberlist,
  transport::resolver::dns::DnsResolver,
};

use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::{UnixListener, UnixStream},
  sync::{mpsc::Sender, oneshot},
};

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
    let delegate = CompositeDelegate::<NodeId, SocketAddr>::default().with_node_delegate(memdb);
    let memberlist = TokioTcpMemberlist::with_delegate(delegate, net_opts, opts).await?;
    tokio::spawn(async move {
      loop {
        tokio::select! {
          _ = tokio::signal::ctrl_c() => {
            tracing::info!("toydb: shutting down db event listener");
          }
          ev = rx.recv() => {
            if let Some(ev) = ev {
              match ev {
                Event::Join { id, addr, tx } => {
                  let res = memberlist.join(Node::new(id, MaybeResolvedAddress::Resolved(addr))).await;
                  let _ = tx.send(res.map_err(Into::into).map(|_| ()));
                }
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
              }
            }
          }
        }
      }
    });

    Ok(Self { tx })
  }

  async fn handle_get<W: tokio::io::AsyncWrite + Unpin>(
    &self,
    key: Bytes,
    stream: &mut W,
  ) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    if let Err(e) = self.tx.send(Event::Get { key, tx }).await {
      tracing::error!(err=%e, "toydb: fail to send get event");
      return Ok(());
    }

    let resp = rx.await?;
    tracing::info!(value=?resp, "toydb: fetch key");
    match bincode::serialize(&resp) {
      Ok(resp) => {
        let mut prefixed_data = vec![0; resp.len() + 4];
        prefixed_data[..4].copy_from_slice(&(resp.len() as u32).to_le_bytes());
        prefixed_data[4..].copy_from_slice(&resp);
        if let Err(e) = stream.write_all(&prefixed_data).await {
          tracing::error!(err=%e, "toydb: fail to write rpc response");
        } else {
          tracing::info!(data=?prefixed_data, "toydb: send get response");
        }
      }
      Err(e) => {
        tracing::error!(err=%e, "toydb: fail to encode rpc response");
      }
    }
    Ok(())
  }

  async fn handle_join<W: tokio::io::AsyncWrite + Unpin>(
    &self,
    id: NodeId,
    addr: SocketAddr,
    stream: &mut W,
  ) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    self
      .tx
      .send(Event::Join {
        id: id.clone(),
        addr,
        tx,
      })
      .await?;

    let resp = rx.await?;
    if let Err(e) = resp {
      let res = std::result::Result::<(), String>::Err(e.to_string());
      match bincode::serialize(&res) {
        Ok(resp) => {
          let mut prefixed_data = vec![0; resp.len() + 4];
          prefixed_data[..4].copy_from_slice(&(resp.len() as u32).to_le_bytes());
          prefixed_data[4..].copy_from_slice(&resp);
          if let Err(e) = stream.write_all(&prefixed_data).await {
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
          let mut prefixed_data = vec![0; resp.len() + 4];
          prefixed_data[..4].copy_from_slice(&(resp.len() as u32).to_le_bytes());
          prefixed_data[4..].copy_from_slice(&resp);
          if let Err(e) = stream.write_all(&prefixed_data).await {
            tracing::error!(err=%e, "toydb: fail to write rpc response");
          }
        }
        Err(e) => {
          tracing::error!(err=%e, "toydb: fail to encode rpc response");
        }
      }
    }

    Ok(())
  }

  async fn handle_insert<W: tokio::io::AsyncWrite + Unpin>(
    &self,
    key: Bytes,
    value: Bytes,
    stream: &mut W,
  ) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    self.tx.send(Event::Set { key, value, tx }).await?;

    let resp = rx.await?;
    if let Err(e) = resp {
      let res = std::result::Result::<(), String>::Err(e.to_string());
      match bincode::serialize(&res) {
        Ok(resp) => {
          let mut prefixed_data = vec![0; resp.len() + 4];
          prefixed_data[..4].copy_from_slice(&(resp.len() as u32).to_le_bytes());
          prefixed_data[4..].copy_from_slice(&resp);
          if let Err(e) = stream.write_all(&prefixed_data).await {
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
          let mut prefixed_data = vec![0; resp.len() + 4];
          prefixed_data[..4].copy_from_slice(&(resp.len() as u32).to_le_bytes());
          prefixed_data[4..].copy_from_slice(&resp);
          if let Err(e) = stream.write_all(&prefixed_data).await {
            tracing::error!(err=%e, "toydb: fail to write rpc response");
          }
        }
        Err(e) => {
          tracing::error!(err=%e, "toydb: fail to encode rpc response");
        }
      }
    }
    Ok(())
  }
}

#[derive(clap::Args)]
struct StartArgs {
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
  #[clap(short, long)]
  rpc_addr: std::path::PathBuf,
}

#[derive(clap::Subcommand)]
enum Commands {
  /// Start the toydb instance
  Start(StartArgs),
  /// Join to an existing toydb cluster
  Join {
    #[clap(short, long)]
    id: NodeId,
    #[clap(short, long)]
    addr: SocketAddr,
    #[clap(short, long)]
    rpc_addr: std::path::PathBuf,
  },
  /// Fetch a value by key from the toydb
  Get {
    #[clap(short, long)]
    key: String,
    #[clap(short, long)]
    rpc_addr: std::path::PathBuf,
  },
  /// Set a key-value to the toydb if the key not exists
  Set {
    #[clap(short, long)]
    key: String,
    #[clap(short, long)]
    value: String,
    #[clap(short, long)]
    rpc_addr: std::path::PathBuf,
  },
}

#[derive(clap::Parser)]
#[command(name = "toydb")]
#[command(about = "CLI for toydb example", long_about = None)]
struct Cli {
  #[clap(subcommand)]
  command: Commands,
}

#[derive(serde::Serialize, serde::Deserialize)]
enum Op {
  Get(Bytes),
  Set(Bytes, Bytes),
  Join { addr: SocketAddr, id: NodeId },
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
  Join {
    addr: SocketAddr,
    id: NodeId,
    tx: oneshot::Sender<Result<()>>,
  },
}

#[tokio::main]
async fn main() -> Result<()> {
  let filter = std::env::var("TOYDB_LOG").unwrap_or_else(|_| "info".to_owned());
  tracing::subscriber::set_global_default(
    tracing_subscriber::fmt::fmt()
      .without_time()
      .with_line_number(true)
      .with_env_filter(filter)
      .with_file(false)
      .with_target(true)
      .with_ansi(true)
      .finish(),
  )
  .unwrap();

  let cli = Cli::parse();
  match cli.command {
    Commands::Join { addr, id, rpc_addr } => {
      handle_join_cmd(id, addr, rpc_addr).await?;
    }
    Commands::Get { key, rpc_addr } => {
      handle_get_cmd(key, rpc_addr).await?;
    }
    Commands::Set {
      key,
      value,
      rpc_addr,
    } => {
      handle_set_cmd(key, value, rpc_addr).await?;
    }
    Commands::Start(args) => {
      handle_start_cmd(args).await?;
    }
  }

  Ok(())
}

async fn handle_join_cmd(id: NodeId, addr: SocketAddr, rpc_addr: std::path::PathBuf) -> Result<()> {
  let conn = UnixStream::connect(rpc_addr).await?;
  let data = serialize(&Op::Join { id, addr })?;

  let (reader, mut writer) = conn.into_split();

  let mut prefixed_data = vec![0; data.len() + 4];
  prefixed_data[..4].copy_from_slice(&(data.len() as u32).to_le_bytes());
  prefixed_data[4..].copy_from_slice(&data);

  writer.write_all(&prefixed_data).await?;
  writer.shutdown().await?;

  let mut reader = tokio::io::BufReader::new(reader);
  let mut len_buf = [0; 4];
  reader.read_exact(&mut len_buf).await?;
  let len = u32::from_le_bytes(len_buf) as usize;

  let mut buf = vec![0; len];
  reader.read_exact(&mut buf).await?;
  let res = deserialize::<std::result::Result<(), String>>(&buf)?;
  match res {
    Ok(_) => {
      println!("join successfully");
    }
    Err(e) => {
      println!("fail to join {e}")
    }
  }
  Ok(())
}

async fn handle_get_cmd(key: String, rpc_addr: std::path::PathBuf) -> Result<()> {
  let conn = UnixStream::connect(rpc_addr).await?;
  let data = serialize(&Op::Get(key.into_bytes().into()))?;

  let (reader, mut writer) = conn.into_split();

  let mut prefixed_data = vec![0; data.len() + 4];
  prefixed_data[..4].copy_from_slice(&(data.len() as u32).to_le_bytes());
  prefixed_data[4..].copy_from_slice(&data);

  writer.write_all(&prefixed_data).await?;
  writer.shutdown().await?;

  let mut reader = tokio::io::BufReader::new(reader);
  let mut len_buf = [0; 4];
  reader.read_exact(&mut len_buf).await?;
  let len = u32::from_le_bytes(len_buf) as usize;

  let mut buf = vec![0; len];
  reader.read_exact(&mut buf).await?;
  let res = deserialize::<Option<Bytes>>(&buf)?;
  match res {
    Some(value) => {
      println!("{}", String::from_utf8_lossy(&value));
    }
    None => {
      println!("key not found");
    }
  }
  Ok(())
}

async fn handle_set_cmd(key: String, value: String, rpc_addr: std::path::PathBuf) -> Result<()> {
  let conn = UnixStream::connect(rpc_addr).await?;
  let data = serialize(&Op::Set(key.into_bytes().into(), value.into_bytes().into()))?;

  let (reader, mut writer) = conn.into_split();

  let mut prefixed_data = vec![0; data.len() + 4];
  prefixed_data[..4].copy_from_slice(&(data.len() as u32).to_le_bytes());
  prefixed_data[4..].copy_from_slice(&data);

  writer.write_all(&prefixed_data).await?;
  writer.shutdown().await?;

  let mut reader = tokio::io::BufReader::new(reader);
  let mut len_buf = [0; 4];
  reader.read_exact(&mut len_buf).await?;
  let len = u32::from_le_bytes(len_buf) as usize;

  let mut buf = vec![0; len];
  reader.read_exact(&mut buf).await?;
  let res = deserialize::<std::result::Result<(), String>>(&buf)?;
  match res {
    Ok(_) => {
      println!("insert successfully");
    }
    Err(e) => {
      println!("fail to insert {e}")
    }
  }
  Ok(())
}

async fn handle_start_cmd(args: StartArgs) -> Result<()> {
  let opts = Options::local();
  let net_opts = NetTransportOptions::new(args.id)
    .with_bind_addresses([HostAddr::from(args.addr)].into_iter().collect());

  let db = ToyDb::new(args.meta, opts, net_opts).await?;

  struct Guard {
    sock: std::path::PathBuf,
  }

  impl Drop for Guard {
    fn drop(&mut self) {
      if let Err(e) = std::fs::remove_file(&self.sock) {
        tracing::error!(err=%e, "toydb: fail to remove rpc sock");
      }
    }
  }

  let _guard = Guard {
    sock: args.rpc_addr.clone(),
  };

  let listener = UnixListener::bind(&args.rpc_addr)?;

  tracing::info!("toydb: start listening on {}", args.rpc_addr.display());

  loop {
    tokio::select! {
      conn = listener.accept() => {
        let (stream, _) = conn?;
        let mut stream = tokio::io::BufReader::new(stream);
        let mut len_buf = [0; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut data = vec![0; len];
        if let Err(e) = stream.read_exact(&mut data).await {
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
          Op::Join { addr, id } => {
            db.handle_join(id, addr, &mut stream).await?;
          }
          Op::Get(key) => {
            db.handle_get(key, &mut stream).await?;
          },
          Op::Set(key, value) => {
            db.handle_insert(key, value, &mut stream).await?;
          },
        }

        if let Err(e) = stream.into_inner().shutdown().await {
          tracing::error!(err=%e, "toydb: fail to shutdown rpc stream");
        }
      }
      _ = tokio::signal::ctrl_c() => {
        break;
      }
    }
  }
  Ok(())
}
