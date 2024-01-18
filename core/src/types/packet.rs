use std::time::Instant;

use bytes::{Buf, BufMut, BytesMut};

#[viewit::viewit(
  vis_all = "",
  getters(vis_all = "pub", style = "ref"),
  setters(vis_all = "pub")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Packet<A> {
  /// The raw contents of the packet.
  #[viewit(getter(skip))]
  buf: BytesMut,

  /// Address of the peer. This is an actual address so we
  /// can expose some concrete details about incoming packets.
  from: A,

  /// The time when the packet was received. This should be
  /// taken as close as possible to the actual receipt time to help make an
  /// accurate RTT measurement during probes.
  timestamp: Instant,
}

impl<A> Packet<A> {
  #[inline]
  pub fn new(buf: BytesMut, from: A, timestamp: Instant) -> Self {
    Self {
      buf,
      from,
      timestamp,
    }
  }

  #[inline]
  pub fn into_components(self) -> (BytesMut, A, Instant) {
    (self.buf, self.from, self.timestamp)
  }

  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    self.buf.as_ref()
  }

  #[inline]
  pub fn as_mut_slice(&mut self) -> &mut [u8] {
    self.buf.as_mut()
  }

  #[inline]
  pub fn into_inner(self) -> BytesMut {
    self.buf
  }

  #[inline]
  pub fn resize(&mut self, new_len: usize, val: u8) {
    self.buf.resize(new_len, val);
  }

  #[inline]
  pub fn reserve(&mut self, additional: usize) {
    self.buf.reserve(additional);
  }

  #[inline]
  pub fn remaining(&self) -> usize {
    self.buf.remaining()
  }

  #[inline]
  pub fn remaining_mut(&self) -> usize {
    self.buf.remaining_mut()
  }

  #[inline]
  pub fn truncate(&mut self, len: usize) {
    self.buf.truncate(len);
  }

  #[inline]
  pub fn put_slice(&mut self, buf: &[u8]) {
    self.buf.put_slice(buf);
  }

  #[inline]
  pub fn put_u8(&mut self, val: u8) {
    self.buf.put_u8(val);
  }

  #[inline]
  pub fn put_u16(&mut self, val: u16) {
    self.buf.put_u16(val);
  }

  #[inline]
  pub fn put_u16_le(&mut self, val: u16) {
    self.buf.put_u16_le(val);
  }

  #[inline]
  pub fn put_u32(&mut self, val: u32) {
    self.buf.put_u32(val);
  }

  #[inline]
  pub fn put_u32_le(&mut self, val: u32) {
    self.buf.put_u32_le(val);
  }

  #[inline]
  pub fn put_u64(&mut self, val: u64) {
    self.buf.put_u64(val);
  }

  #[inline]
  pub fn put_u64_le(&mut self, val: u64) {
    self.buf.put_u64_le(val);
  }

  #[inline]
  pub fn put_i8(&mut self, val: i8) {
    self.buf.put_i8(val);
  }

  #[inline]
  pub fn put_i16(&mut self, val: i16) {
    self.buf.put_i16(val);
  }

  #[inline]
  pub fn put_i16_le(&mut self, val: i16) {
    self.buf.put_i16_le(val);
  }

  #[inline]
  pub fn put_i32(&mut self, val: i32) {
    self.buf.put_i32(val);
  }

  #[inline]
  pub fn put_i32_le(&mut self, val: i32) {
    self.buf.put_i32_le(val);
  }

  #[inline]
  pub fn put_i64(&mut self, val: i64) {
    self.buf.put_i64(val);
  }

  #[inline]
  pub fn put_i64_le(&mut self, val: i64) {
    self.buf.put_i64_le(val);
  }

  #[inline]
  pub fn put_f32(&mut self, val: f32) {
    self.buf.put_f32(val);
  }

  #[inline]
  pub fn put_f32_le(&mut self, val: f32) {
    self.buf.put_f32_le(val);
  }

  #[inline]
  pub fn put_f64(&mut self, val: f64) {
    self.buf.put_f64(val);
  }

  #[inline]
  pub fn put_f64_le(&mut self, val: f64) {
    self.buf.put_f64_le(val);
  }

  #[inline]
  pub fn put_bool(&mut self, val: bool) {
    self.buf.put_u8(val as u8);
  }

  #[inline]
  pub fn put_bytes(&mut self, val: u8, cnt: usize) {
    self.buf.put_bytes(val, cnt);
  }

  #[inline]
  pub fn clear(&mut self) {
    self.buf.clear();
  }
}
