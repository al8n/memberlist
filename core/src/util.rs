pub use const_varint::*;
pub use is_global_ip::IsGlobalIp;

use crate::types::{Data, Message, SmallVec, TinyVec};

pub(crate) fn retransmit_limit(retransmit_mult: usize, n: usize) -> usize {
  let node_scale = ((n + 1) as f64).log10().ceil() as usize;
  retransmit_mult * node_scale
}

/// Returns the hostname of the current machine.
///
/// On wasm target, this function always returns `None`.
///
/// # Examples
///
/// ```
/// use memberlist_core::util::hostname;
///
/// let hostname = hostname();
/// println!("hostname: {hostname:?}");
/// ```
#[allow(unreachable_code)]
pub fn hostname() -> Option<String> {
  #[cfg(not(any(target_arch = "wasm32", windows)))]
  return {
    let name = rustix::system::uname();
    let name = name.nodename();
    name
      .is_empty()
      .then_some(name.to_string_lossy().to_string())
  };

  #[cfg(windows)]
  return {
    match ::hostname::get() {
      Ok(name) => {
        let name = name.to_string_lossy();
        name.is_empty().then_some(name.to_string())
      }
      Err(_) => None,
    }
  };

  None
}

/// A batch of messages.
#[derive(Debug, Clone)]
pub enum Batch<I, A> {
  /// Batch contains only one [`Message`].
  One {
    /// The message in this batch.
    msg: Message<I, A>,
    /// The estimated encoded size of this [`Message`].
    estimate_encoded_size: usize,
  },
  /// Batch contains multiple [`Message`]s.
  More {
    /// The estimated encoded size of this batch.
    estimate_encoded_size: usize,
    /// The messages in this batch.
    msgs: TinyVec<Message<I, A>>,
  },
}

impl<I, A> IntoIterator for Batch<I, A> {
  type Item = Message<I, A>;

  type IntoIter = <TinyVec<Message<I, A>> as IntoIterator>::IntoIter;

  fn into_iter(self) -> Self::IntoIter {
    match self {
      Self::One { msg, .. } => TinyVec::from(msg).into_iter(),
      Self::More { msgs, .. } => msgs.into_iter(),
    }
  }
}

impl<I, A> Batch<I, A> {
  /// Returns the estimated encoded size for this batch.
  #[inline]
  pub const fn estimate_encoded_size(&self) -> usize {
    match self {
      Self::One {
        estimate_encoded_size,
        ..
      } => *estimate_encoded_size,
      Self::More {
        estimate_encoded_size,
        ..
      } => *estimate_encoded_size,
    }
  }

  /// Returns the number of messages in this batch.
  #[inline]
  pub fn len(&self) -> usize {
    match self {
      Self::One { .. } => 1,
      Self::More { msgs, .. } => msgs.len(),
    }
  }

  /// Returns `true` if this batch is empty.
  #[inline]
  pub fn is_empty(&self) -> bool {
    match self {
      Self::One { .. } => false,
      Self::More { msgs, .. } => msgs.is_empty(),
    }
  }
}

/// Used to indicate how to batch a collection of messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchHint {
  /// Batch should contains only one [`Message`]
  One {
    /// The index of this message belongs to the original slice
    idx: usize,
    /// The encoded size of this message
    encoded_size: usize,
  },
  /// Batch should contains multiple  [`Message`]s
  More {
    /// The range of this batch belongs to the original slice
    range: core::ops::Range<usize>,
    /// The encoded size of this batch
    encoded_size: usize,
  },
}

/// Calculate batch hints for a slice of messages.
fn batch_hints<I, A>(
  fixed_payload_overhead: usize,
  batch_overhead: usize,
  msg_overhead: usize,
  max_encoded_batch_size: usize,
  max_encoded_message_size: usize,
  max_messages_per_batch: usize,
  msgs: &[Message<I, A>],
) -> SmallVec<BatchHint>
where
  I: Data + Send + Sync + 'static,
  A: Data + Send + Sync + 'static,
{
  let mut infos = SmallVec::new();
  let mut current_encoded_size = fixed_payload_overhead + batch_overhead;
  let mut batch_start_idx = 0;
  let total_len = msgs.len();

  if total_len == 0 {
    return infos;
  }

  if total_len == 1 {
    let msg_encoded_len = msgs[0].encoded_len();
    infos.push(BatchHint::One {
      idx: 0,
      encoded_size: fixed_payload_overhead + msg_encoded_len,
    });
    return infos;
  }

  let mut current_num_packets_in_batch = 0;
  for (idx, msg) in msgs.iter().enumerate() {
    let msg_encoded_len = msg.encoded_len();
    if msg_encoded_len > max_encoded_message_size {
      infos.push(BatchHint::One {
        idx,
        encoded_size: fixed_payload_overhead + msg_encoded_len,
      });
      continue;
    }

    let need = msg_overhead + msg_encoded_len;

    // if we are the last one, we need to finish the current batch anyway.
    if idx + 1 == total_len {
      // If we cannot fit in the current batch because of max encoded size reached
      if need + current_encoded_size > max_encoded_batch_size {
        // finish the current batch
        infos.push(BatchHint::More {
          range: batch_start_idx..idx,
          encoded_size: current_encoded_size,
        });

        // add the last one to a new batch which only contains one message
        infos.push(BatchHint::One {
          idx,
          encoded_size: fixed_payload_overhead + msg_encoded_len,
        });
        return infos;
      }

      // If we cannot fit in the current batch because of max packets per batch reached
      if current_num_packets_in_batch >= max_messages_per_batch {
        // finish the current batch
        infos.push(BatchHint::More {
          range: batch_start_idx..idx,
          encoded_size: current_encoded_size,
        });

        // add the last one to a new batch which only contains one message
        infos.push(BatchHint::One {
          idx,
          encoded_size: fixed_payload_overhead + msg_encoded_len,
        });
        return infos;
      }

      infos.push(BatchHint::More {
        range: batch_start_idx..idx + 1,
        encoded_size: current_encoded_size + need,
      });
      return infos;
    }

    // if the current packet cannot fit in the current batch because of max encoded size reached
    if need + current_encoded_size > max_encoded_batch_size {
      // finish the current batch
      infos.push(BatchHint::More {
        range: batch_start_idx..idx,
        encoded_size: current_encoded_size,
      });

      // start a new batch
      current_encoded_size = fixed_payload_overhead + batch_overhead + need;
      current_num_packets_in_batch = 1;
      batch_start_idx = idx;
      continue;
    }

    // if the current packet cannot fit in the current batch because of max packets per batch reached
    if current_num_packets_in_batch >= max_messages_per_batch {
      // finish the current batch
      infos.push(BatchHint::More {
        range: batch_start_idx..idx,
        encoded_size: current_encoded_size,
      });

      // start a new batch
      current_encoded_size = fixed_payload_overhead + batch_overhead + need;
      current_num_packets_in_batch = 1;
      batch_start_idx = idx;
      continue;
    }

    current_num_packets_in_batch += 1;
    current_encoded_size += need;
  }

  infos
}

/// Batch a collection of messages.
pub fn batch<I, A, M>(
  fixed_payload_overhead: usize,
  batch_overhead: usize,
  msg_overhead: usize,
  max_encoded_batch_size: usize,
  max_encoded_message_size: usize,
  max_messages_per_batch: usize,
  msgs: M,
) -> SmallVec<Batch<I, A>>
where
  I: Data + Send + Sync + 'static,
  A: Data + Send + Sync + 'static,
  M: AsRef<[Message<I, A>]> + IntoIterator<Item = Message<I, A>>,
{
  let hints = batch_hints(
    fixed_payload_overhead,
    batch_overhead,
    msg_overhead,
    max_encoded_batch_size,
    max_encoded_message_size,
    max_messages_per_batch,
    msgs.as_ref(),
  );
  let mut batches = SmallVec::with_capacity(hints.len());
  let mut msgs = msgs.into_iter();
  for hint in hints {
    match hint {
      BatchHint::One { encoded_size, .. } => {
        batches.push(Batch::One {
          msg: msgs.next().unwrap(),
          estimate_encoded_size: encoded_size,
        });
      }
      BatchHint::More {
        range,
        encoded_size,
      } => {
        let mut batch = TinyVec::with_capacity(range.end - range.start);
        for _ in range {
          batch.push(msgs.next().unwrap());
        }
        batches.push(Batch::More {
          estimate_encoded_size: encoded_size,
          msgs: batch,
        });
      }
    }
  }
  batches
}

/// The code in this mod is copied from [libp2p]
///
/// [libp2p]: https://github.com/tcoratger/rust-libp2p/blob/master/core/src/transport/global_only.rs.
mod is_global_ip {
  /// A trait for checking if an IP address is globally reachable.
  pub trait IsGlobalIp {
    /// Returns [`true`] if the address appears to be globally reachable
    fn is_global_ip(&self) -> bool;
  }

  impl IsGlobalIp for std::net::Ipv4Addr {
    #[inline]
    fn is_global_ip(&self) -> bool {
      ipv4_global::is_global(*self)
    }
  }

  impl IsGlobalIp for std::net::Ipv6Addr {
    #[inline]
    fn is_global_ip(&self) -> bool {
      ipv6_global::is_global(*self)
    }
  }

  impl IsGlobalIp for std::net::IpAddr {
    #[inline]
    fn is_global_ip(&self) -> bool {
      match self {
        std::net::IpAddr::V4(addr) => addr.is_global_ip(),
        std::net::IpAddr::V6(addr) => addr.is_global_ip(),
      }
    }
  }

  impl IsGlobalIp for std::net::SocketAddrV4 {
    #[inline]
    fn is_global_ip(&self) -> bool {
      self.ip().is_global_ip()
    }
  }

  impl IsGlobalIp for std::net::SocketAddrV6 {
    #[inline]
    fn is_global_ip(&self) -> bool {
      self.ip().is_global_ip()
    }
  }

  impl IsGlobalIp for std::net::SocketAddr {
    #[inline]
    fn is_global_ip(&self) -> bool {
      self.ip().is_global_ip()
    }
  }

  /// This module contains an implementation of the `is_global` IPv4 address space.
  ///
  /// Credit for this implementation goes to the Rust standard library team.
  ///
  /// Unstable tracking issue: [#27709](https://github.com/rust-lang/rust/issues/27709)
  mod ipv4_global {
    use std::net::Ipv4Addr;

    /// Returns [`true`] if this address is reserved by IANA for future use. [IETF RFC 1112]
    /// defines the block of reserved addresses as `240.0.0.0/4`. This range normally includes the
    /// broadcast address `255.255.255.255`, but this implementation explicitly excludes it, since
    /// it is obviously not reserved for future use.
    ///
    /// [IETF RFC 1112]: https://tools.ietf.org/html/rfc1112
    ///
    /// # Warning
    ///
    /// As IANA assigns new addresses, this method will be
    /// updated. This may result in non-reserved addresses being
    /// treated as reserved in code that relies on an outdated version
    /// of this method.
    #[must_use]
    #[inline]
    const fn is_reserved(a: Ipv4Addr) -> bool {
      a.octets()[0] & 240 == 240 && !a.is_broadcast()
    }

    /// Returns [`true`] if this address part of the `198.18.0.0/15` range, which is reserved for
    /// network devices benchmarking. This range is defined in [IETF RFC 2544] as `192.18.0.0`
    /// through `198.19.255.255` but [errata 423] corrects it to `198.18.0.0/15`.
    ///
    /// [IETF RFC 2544]: https://tools.ietf.org/html/rfc2544
    /// [errata 423]: https://www.rfc-editor.org/errata/eid423
    #[must_use]
    #[inline]
    const fn is_benchmarking(a: Ipv4Addr) -> bool {
      a.octets()[0] == 198 && (a.octets()[1] & 0xfe) == 18
    }

    /// Returns [`true`] if this address is part of the Shared Address Space defined in
    /// [IETF RFC 6598] (`100.64.0.0/10`).
    ///
    /// [IETF RFC 6598]: https://tools.ietf.org/html/rfc6598
    #[must_use]
    #[inline]
    const fn is_shared(a: Ipv4Addr) -> bool {
      a.octets()[0] == 100 && (a.octets()[1] & 0b1100_0000 == 0b0100_0000)
    }

    /// Returns [`true`] if this is a private address.
    ///
    /// The private address ranges are defined in [IETF RFC 1918] and include:
    ///
    ///  - `10.0.0.0/8`
    ///  - `172.16.0.0/12`
    ///  - `192.168.0.0/16`
    ///
    /// [IETF RFC 1918]: https://tools.ietf.org/html/rfc1918
    #[must_use]
    #[inline]
    const fn is_private(a: Ipv4Addr) -> bool {
      match a.octets() {
        [10, ..] => true,
        [172, b, ..] if b >= 16 && b <= 31 => true,
        [192, 168, ..] => true,
        _ => false,
      }
    }

    /// Returns [`true`] if the address appears to be globally reachable
    /// as specified by the [IANA IPv4 Special-Purpose Address Registry].
    /// Whether or not an address is practically reachable will depend on your network configuration.
    ///
    /// Most IPv4 addresses are globally reachable;
    /// unless they are specifically defined as *not* globally reachable.
    ///
    /// Non-exhaustive list of notable addresses that are not globally reachable:
    ///
    /// - The [unspecified address] ([`is_unspecified`](Ipv4Addr::is_unspecified))
    /// - Addresses reserved for private use ([`is_private`](Ipv4Addr::is_private))
    /// - Addresses in the shared address space ([`is_shared`](Ipv4Addr::is_shared))
    /// - Loopback addresses ([`is_loopback`](Ipv4Addr::is_loopback))
    /// - Link-local addresses ([`is_link_local`](Ipv4Addr::is_link_local))
    /// - Addresses reserved for documentation ([`is_documentation`](Ipv4Addr::is_documentation))
    /// - Addresses reserved for benchmarking ([`is_benchmarking`](Ipv4Addr::is_benchmarking))
    /// - Reserved addresses ([`is_reserved`](Ipv4Addr::is_reserved))
    /// - The [broadcast address] ([`is_broadcast`](Ipv4Addr::is_broadcast))
    ///
    /// For the complete overview of which addresses are globally reachable, see the table at the [IANA IPv4 Special-Purpose Address Registry].
    ///
    /// [IANA IPv4 Special-Purpose Address Registry]: https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml
    /// [unspecified address]: Ipv4Addr::UNSPECIFIED
    /// [broadcast address]: Ipv4Addr::BROADCAST
    #[must_use]
    #[inline]
    pub(crate) const fn is_global(a: Ipv4Addr) -> bool {
      !(a.octets()[0] == 0 // "This network"
          || is_private(a)
          || is_shared(a)
          || a.is_loopback()
          || a.is_link_local()
          // addresses reserved for future protocols (`192.0.0.0/24`)
          ||(a.octets()[0] == 192 && a.octets()[1] == 0 && a.octets()[2] == 0)
          || a.is_documentation()
          || is_benchmarking(a)
          || is_reserved(a)
          || a.is_broadcast())
    }
  }

  /// This module contains an implementation of the `is_global` IPv6 address space.
  ///
  /// Credit for this implementation goes to the Rust standard library team.
  ///
  /// Unstable tracking issue: [#27709](https://github.com/rust-lang/rust/issues/27709)
  mod ipv6_global {
    use std::net::Ipv6Addr;

    /// Returns `true` if the address is a unicast address with link-local scope,
    /// as defined in [RFC 4291].
    ///
    /// A unicast address has link-local scope if it has the prefix `fe80::/10`, as per [RFC 4291 section 2.4].
    /// Note that this encompasses more addresses than those defined in [RFC 4291 section 2.5.6],
    /// which describes "Link-Local IPv6 Unicast Addresses" as having the following stricter format:
    ///
    /// ```text
    /// | 10 bits  |         54 bits         |          64 bits           |
    /// +----------+-------------------------+----------------------------+
    /// |1111111010|           0             |       interface ID         |
    /// +----------+-------------------------+----------------------------+
    /// ```
    /// So while currently the only addresses with link-local scope an application will encounter are all in `fe80::/64`,
    /// this might change in the future with the publication of new standards. More addresses in `fe80::/10` could be allocated,
    /// and those addresses will have link-local scope.
    ///
    /// Also note that while [RFC 4291 section 2.5.3] mentions about the [loopback address] (`::1`) that "it is treated as having Link-Local scope",
    /// this does not mean that the loopback address actually has link-local scope and this method will return `false` on it.
    ///
    /// [RFC 4291]: https://tools.ietf.org/html/rfc4291
    /// [RFC 4291 section 2.4]: https://tools.ietf.org/html/rfc4291#section-2.4
    /// [RFC 4291 section 2.5.3]: https://tools.ietf.org/html/rfc4291#section-2.5.3
    /// [RFC 4291 section 2.5.6]: https://tools.ietf.org/html/rfc4291#section-2.5.6
    /// [loopback address]: Ipv6Addr::LOCALHOST
    #[must_use]
    #[inline]
    const fn is_unicast_link_local(a: Ipv6Addr) -> bool {
      (a.segments()[0] & 0xffc0) == 0xfe80
    }

    /// Returns [`true`] if this is a unique local address (`fc00::/7`).
    ///
    /// This property is defined in [IETF RFC 4193].
    ///
    /// [IETF RFC 4193]: https://tools.ietf.org/html/rfc4193
    #[must_use]
    #[inline]
    const fn is_unique_local(a: Ipv6Addr) -> bool {
      (a.segments()[0] & 0xfe00) == 0xfc00
    }

    /// Returns [`true`] if this is an address reserved for documentation
    /// (`2001:db8::/32`).
    ///
    /// This property is defined in [IETF RFC 3849].
    ///
    /// [IETF RFC 3849]: https://tools.ietf.org/html/rfc3849
    #[must_use]
    #[inline]
    const fn is_documentation(a: Ipv6Addr) -> bool {
      (a.segments()[0] == 0x2001) && (a.segments()[1] == 0xdb8)
    }

    /// Returns [`true`] if the address appears to be globally reachable
    /// as specified by the [IANA IPv6 Special-Purpose Address Registry].
    /// Whether or not an address is practically reachable will depend on your network configuration.
    ///
    /// Most IPv6 addresses are globally reachable;
    /// unless they are specifically defined as *not* globally reachable.
    ///
    /// Non-exhaustive list of notable addresses that are not globally reachable:
    /// - The [unspecified address] ([`is_unspecified`](Ipv6Addr::is_unspecified))
    /// - The [loopback address] ([`is_loopback`](Ipv6Addr::is_loopback))
    /// - IPv4-mapped addresses
    /// - Addresses reserved for benchmarking
    /// - Addresses reserved for documentation ([`is_documentation`](Ipv6Addr::is_documentation))
    /// - Unique local addresses ([`is_unique_local`](Ipv6Addr::is_unique_local))
    /// - Unicast addresses with link-local scope ([`is_unicast_link_local`](Ipv6Addr::is_unicast_link_local))
    ///
    /// For the complete overview of which addresses are globally reachable, see the table at the [IANA IPv6 Special-Purpose Address Registry].
    ///
    /// Note that an address having global scope is not the same as being globally reachable,
    /// and there is no direct relation between the two concepts: There exist addresses with global scope
    /// that are not globally reachable (for example unique local addresses),
    /// and addresses that are globally reachable without having global scope
    /// (multicast addresses with non-global scope).
    ///
    /// [IANA IPv6 Special-Purpose Address Registry]: https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml
    /// [unspecified address]: Ipv6Addr::UNSPECIFIED
    /// [loopback address]: Ipv6Addr::LOCALHOST
    #[must_use]
    #[inline]
    pub(crate) const fn is_global(a: Ipv6Addr) -> bool {
      !(a.is_unspecified()
          || a.is_loopback()
          // IPv4-mapped Address (`::ffff:0:0/96`)
          || matches!(a.segments(), [0, 0, 0, 0, 0, 0xffff, _, _])
          // IPv4-IPv6 Translat. (`64:ff9b:1::/48`)
          || matches!(a.segments(), [0x64, 0xff9b, 1, _, _, _, _, _])
          // Discard-Only Address Block (`100::/64`)
          || matches!(a.segments(), [0x100, 0, 0, 0, _, _, _, _])
          // IETF Protocol Assignments (`2001::/23`)
          || (matches!(a.segments(), [0x2001, b, _, _, _, _, _, _] if b < 0x200)
              && !(
                  // Port Control Protocol Anycast (`2001:1::1`)
                  u128::from_be_bytes(a.octets()) == 0x2001_0001_0000_0000_0000_0000_0000_0001
                  // Traversal Using Relays around NAT Anycast (`2001:1::2`)
                  || u128::from_be_bytes(a.octets()) == 0x2001_0001_0000_0000_0000_0000_0000_0002
                  // AMT (`2001:3::/32`)
                  || matches!(a.segments(), [0x2001, 3, _, _, _, _, _, _])
                  // AS112-v6 (`2001:4:112::/48`)
                  || matches!(a.segments(), [0x2001, 4, 0x112, _, _, _, _, _])
                  // ORCHIDv2 (`2001:20::/28`)
                  || matches!(a.segments(), [0x2001, b, _, _, _, _, _, _] if b >= 0x20 && b <= 0x2F)
              ))
          || is_documentation(a)
          || is_unique_local(a)
          || is_unicast_link_local(a))
    }
  }
}

// #[test]
// fn test_batch() {
//   use crate::transport::{Lpe, Wire};
//   use smol_str::SmolStr;
//   use std::net::SocketAddr;

//   let single = Message::<SmolStr, SocketAddr>::UserData("ping".into());
//   let encoded_len = Lpe::<_, _>::encoded_len(&single);
//   let batches = batch::<_, _, _, Lpe<_, _>>(
//     0,
//     2,
//     2,
//     1400,
//     u16::MAX as usize,
//     255,
//     SmallVec::from(single),
//   );
//   assert_eq!(batches.len(), 1, "bad len {}", batches.len());
//   assert_eq!(
//     batches[0].estimate_encoded_size(),
//     encoded_len,
//     "bad estimate len"
//   );

//   let mut total_encoded_len = 0;
//   let bcasts = (0..256)
//     .map(|i| {
//       let msg = Message::UserData(i.to_string().as_bytes().to_vec().into());
//       let encoded_len = Lpe::<_, _>::encoded_len(&msg);
//       total_encoded_len += 2 + encoded_len;
//       msg
//     })
//     .collect::<SmallVec<Message<SmolStr, SocketAddr>>>();

//   let batches = batch::<_, _, _, Lpe<_, _>>(0, 2, 2, 1400, u16::MAX as usize, 255, bcasts);
//   assert_eq!(batches.len(), 2, "bad len {}", batches.len());
//   assert_eq!(batches[0].len() + batches[1].len(), 256, "missing packets");
//   assert_eq!(
//     batches[0].estimate_encoded_size() + batches[1].estimate_encoded_size(),
//     total_encoded_len + 2 + 2,
//     "bad estimate len"
//   );
// }

// #[test]
// fn test_batch_large_max_encoded_batch_size() {
//   use crate::transport::{Lpe, Wire};
//   use smol_str::SmolStr;
//   use std::net::SocketAddr;

//   let mut total_encoded_len = 0;
//   let mut last_one_encoded_len = 0;
//   let bcasts = (0..256)
//     .map(|i| {
//       let msg = Message::UserData(i.to_string().as_bytes().to_vec().into());
//       let encoded_len = Lpe::<_, _>::encoded_len(&msg);
//       if i == 255 {
//         last_one_encoded_len = encoded_len;
//       } else {
//         total_encoded_len += encoded_len;
//       }
//       msg
//     })
//     .collect::<SmallVec<Message<SmolStr, SocketAddr>>>();

//   let batches =
//     batch::<_, _, _, Lpe<_, _>>(0, 6, 4, u32::MAX as usize, u32::MAX as usize, 255, bcasts);
//   assert_eq!(batches.len(), 2, "bad len {}", batches.len());
//   assert_eq!(batches[0].len() + batches[1].len(), 256, "missing packets");
//   assert_eq!(
//     batches[0].estimate_encoded_size(),
//     6 + batches[0].len() * 4 + total_encoded_len,
//     "bad encoded len for batch 0"
//   );
//   assert_eq!(
//     batches[1].estimate_encoded_size(),
//     last_one_encoded_len,
//     "bad encoded len for batch 1"
//   );
//   assert_eq!(
//     batches[0].estimate_encoded_size() + batches[1].estimate_encoded_size(),
//     6 + batches[0].len() * 4 + total_encoded_len + last_one_encoded_len,
//     "bad estimate len"
//   );
// }

// #[test]
// fn test_retransmit_limit() {
//   let lim = retransmit_limit(3, 0);
//   assert_eq!(lim, 0, "bad val {lim}");

//   let lim = retransmit_limit(3, 1);
//   assert_eq!(lim, 3, "bad val {lim}");

//   let lim = retransmit_limit(3, 99);
//   assert_eq!(lim, 6, "bad val {lim}");
// }
