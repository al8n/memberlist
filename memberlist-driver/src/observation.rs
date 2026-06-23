//! Observation byte-backstop accounting shared by the memberlist async driver crates.

use memberlist_proto::event::Event;

/// The observation-channel byte-backstop weight of an event: `Some(len)` for the
/// payload-bearing variants (`UserPacket` / `RemoteStateReceived`, whose `Bytes`
/// ride up to `max_stream_frame_size`), `None` for the small membership / control
/// events the count cap already bounds.
#[must_use]
pub fn observation_payload_bytes<I, A>(ev: &Event<I, A>) -> Option<u64> {
  match ev {
    Event::UserPacket(p) => Some(p.data_ref().len() as u64),
    Event::RemoteStateReceived(r) => Some(r.user_data_ref().len() as u64),
    _ => None,
  }
}
