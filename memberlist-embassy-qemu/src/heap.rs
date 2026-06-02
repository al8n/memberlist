//! The global allocator over a static heap.
//!
//! The memberlist driver is `no_std + alloc`, so the firmware must register a
//! global allocator. [`embedded_alloc::LlffHeap`] manages a fixed static byte
//! region; [`init`] hands it that region once at startup.

use core::mem::MaybeUninit;

use embedded_alloc::LlffHeap as Heap;

/// Backing heap size in bytes.
///
/// Sized for two embassy-net stacks plus two memberlist engines (gossip codec
/// scratch, the reliable-plane connection state, membership maps) running
/// concurrently on the AN386's 4 MB RAM. Generous because the machine has the
/// room and convergence allocates transiently; the static socket/stack buffers
/// live outside this heap (in `.bss`).
pub const HEAP_SIZE: usize = 512 * 1024;

#[global_allocator]
static HEAP: Heap = Heap::empty();

/// The heap's backing storage, in `.bss`.
static mut HEAP_MEM: [MaybeUninit<u8>; HEAP_SIZE] = [MaybeUninit::uninit(); HEAP_SIZE];

/// Initialize the global allocator. Call once, before any allocation.
///
/// # Safety
///
/// Must be called exactly once and before the first heap allocation. The
/// `addr_of_mut!` read of the `static mut` produces a single pointer that is
/// handed to the allocator, which thereafter owns the region; no other code
/// touches `HEAP_MEM`.
pub fn init() {
  // SAFETY: called once at the top of `main` before anything allocates, and
  // `HEAP_MEM` is referenced nowhere else, so this is the sole live reference to
  // the static. `HEAP.init` records the base/size and the allocator owns it from
  // here on.
  unsafe {
    let ptr = core::ptr::addr_of_mut!(HEAP_MEM) as usize;
    HEAP.init(ptr, HEAP_SIZE);
  }
}
