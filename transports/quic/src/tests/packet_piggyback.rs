use memberlist_core::transport::{tests::send_packet_piggyback, Lpe};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone};

use crate::{QuicTransport, QuicTransportOptions, StreamLayer};

use super::*;

#[cfg(feature = "compression")]
pub async fn packet_piggyback<S, R>(s: S, c: S, kind: AddressKind) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let name = format!("{kind}_packet_piggyback");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::new(name.into())
    .with_compressor(Some(Compressor::default()))
    .with_label(label.cheap_clone());
  opts.add_bind_address(kind.next());
  let trans =
    QuicTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts).await?;
  let remote_addr = trans.advertise_address();
  let client = QuicTransportTestClient::<_, R>::new(kind.next(), *remote_addr, c)
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
