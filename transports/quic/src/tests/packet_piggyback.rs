use memberlist_core::transport::{tests::send_packet_piggyback, Lpe};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone};

use crate::{QuicTransport, QuicTransportOptions, StreamLayer};

use super::*;

#[cfg(feature = "compression")]
pub async fn packet_piggyback<S, R>(
  s: S::Options,
  c: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  let name = format!("{kind}_packet_piggyback");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_compressor(Some(Compressor::default()))
    .with_label(label.cheap_clone());
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, _>::new(opts).await?;
  let remote_addr = trans.advertise_address();
  let c = S::new(c).await.unwrap();
  let client = QuicTransportTestClient::<_, R>::new(kind.next(0), *remote_addr, c)
    .await?
    .with_label(label)
    .with_send_label(true)
    .with_send_compressed(Some(Compressor::default()))
    .with_receive_compressed(true)
    .with_receive_verify_label(true);
  send_packet_piggyback(trans, client, |mut src| {
    let tag = src.get_u8();
    assert_eq!(tag, Message::<SmolStr, SocketAddr>::COMPOUND_TAG);
    let _total_len = NetworkEndian::read_u32(&src) as usize;
    src.advance(4);
    let len = src.get_u8();
    assert_eq!(len, 2);

    let m1_len = NetworkEndian::read_u32(&src) as usize;
    src.advance(4);
    let m1 = src.split_to(m1_len);
    let (_, m1) = Message::decode(&m1)?;

    let m2_len = NetworkEndian::read_u32(&src) as usize;
    src.advance(4);
    let m2 = src.split_to(m2_len);
    let (_, m2) = Message::decode(&m2)?;
    Ok([m1, m2])
  })
  .await?;
  Ok(())
}
