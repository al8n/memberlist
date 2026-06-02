//! Bare-metal QEMU execution proof for the `memberlist-embassy` driver.
//!
//! Boots a Cortex-M4 under QEMU's `mps2-an386` machine, stands up the embassy
//! thread-mode executor on a SysTick time driver, and runs two memberlist nodes
//! over an in-memory paired embassy-net link until they converge on a two-member
//! view — then exits 0 via semihosting. A deadline guard exits non-zero if they
//! fail to converge, so the qemu process exit code is the pass/fail signal.
//!
//! This is the runtime counterpart to the host loopback tests in
//! `memberlist-embassy/tests/loopback.rs`: same two-node join, but on a real
//! emulated core with no `std`.

#![no_std]
#![no_main]

extern crate alloc;

mod heap;
mod node;
mod paired_device;
mod rng;
mod systick_driver;

use cortex_m_rt::entry;
use embassy_executor::Executor;
use static_cell::StaticCell;

/// The thread-mode executor, owned for the program's lifetime.
static EXECUTOR: StaticCell<Executor> = StaticCell::new();

#[entry]
fn main() -> ! {
  // Heap first: the driver is `alloc`-based, so nothing may allocate before this.
  heap::init();

  // SysTick time driver: take the core peripherals, hand SysTick to the driver so
  // `embassy_time` has a clock and the executor wakes each tick.
  let core = cortex_m::Peripherals::take().expect("core peripherals available once at boot");
  systick_driver::init(core.SYST);

  // Start the thread-mode executor; `run` never returns. The spawned task drives
  // the whole proof and calls `semihosting::process::exit`, which (with qemu
  // semihosting on) terminates the emulator with that code.
  let executor = EXECUTOR.init(Executor::new());
  executor.run(|spawner| {
    // The entry task receives the `Spawner` so it can launch the four run loops
    // (two memberlist Runners + two embassy-net Runners) once it has built them.
    // `must_spawn`: this single-slot task is spawned exactly once.
    spawner.must_spawn(node::main_task(spawner));
  });
}
