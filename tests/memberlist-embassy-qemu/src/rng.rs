//! A fixed-seed `getrandom` custom backend.
//!
//! The emulated Cortex-M has no hardware RNG, so the gossip layer's default
//! entropy draw (via [`getrandom`]) needs a backend symbol. This crate builds
//! with `--cfg getrandom_backend="custom"`, which makes `getrandom` call the
//! externally defined `__getrandom_v03_custom`; this module provides it.
//!
//! The stream is a deterministic SplitMix64 seeded from a fixed constant. That is
//! intentional: this is a reproducible convergence test, not a security context,
//! so a fixed seed makes every run identical. (Each node additionally receives
//! its own gossip RNG as a seeded `SmallRng` passed to `Memberlist::new_with_rng`,
//! so this backend only ever services whatever incidental entropy the protocol
//! draws beyond that.)
//!
//! The state is a `Cell<u64>` behind a `critical_section::Mutex` rather than an
//! atomic, because ARMv7-M has no 64-bit atomic and this runs on a single core.

use core::cell::Cell;

use critical_section::Mutex;

/// SplitMix64 state, advanced on every fill. A fixed start makes the run
/// deterministic.
static STATE: Mutex<Cell<u64>> = Mutex::new(Cell::new(0x0123_4567_89AB_CDEF));

/// One SplitMix64 step under the critical section: mix and return the next value.
fn next_u64() -> u64 {
  critical_section::with(|cs| {
    let cell = STATE.borrow(cs);
    let z = cell.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
    cell.set(z);
    let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
  })
}

/// The `getrandom` custom backend entry point.
///
/// `getrandom` (built with `getrandom_backend="custom"`) links its `fill_inner`
/// to this symbol. Fills `len` bytes at `dest` from the SplitMix64 stream and
/// reports success.
///
/// # Safety
///
/// `dest` must point to at least `len` writable bytes (guaranteed by `getrandom`,
/// which derives both from a `&mut [MaybeUninit<u8>]`). This writes exactly `len`
/// bytes and reads nothing from `dest`.
#[unsafe(no_mangle)]
unsafe extern "Rust" fn __getrandom_v03_custom(
  dest: *mut u8,
  len: usize,
) -> Result<(), getrandom::Error> {
  let mut written = 0usize;
  while written < len {
    let chunk = next_u64().to_le_bytes();
    let take = core::cmp::min(8, len - written);
    // SAFETY: `dest + written` stays within the `len`-byte region the caller
    // guarantees, and `take <= 8` so this never reads past `chunk`.
    unsafe {
      core::ptr::copy_nonoverlapping(chunk.as_ptr(), dest.add(written), take);
    }
    written += take;
  }
  Ok(())
}
