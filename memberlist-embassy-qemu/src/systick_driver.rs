//! A SysTick-backed [`embassy_time_driver::Driver`] for the emulated Cortex-M.
//!
//! embassy-time 0.5 with the `generic-queue-8` feature does *not* ship a
//! `schedule_wake` implementation on a bare-metal target — it only provides the
//! 8-slot software timer queue ([`embassy_time_queue_utils::Queue`]). This module
//! supplies the missing half: a full [`Driver`] (both [`Driver::now`] and
//! [`Driver::schedule_wake`]) driven by the core's SysTick exception.
//!
//! # How time advances
//!
//! [`init`] programs SysTick to fire periodically, and the handler maintains a
//! monotonic 64-bit tick counter that [`Driver::now`] reads. Because the gossip
//! protocol only ever measures *relative* intervals, the absolute rate of this
//! virtual clock against wall-clock time is irrelevant; all that matters is that
//! it is monotonic and that it advances whenever the executor is parked. SysTick
//! gives exactly that: it is an exception, so it resumes the thread-mode executor
//! out of its `WFE` sleep on every tick.
//!
//! # How timers fire
//!
//! [`Driver::schedule_wake`] stores the waker in the queue. There is no separate
//! hardware compare/alarm to program — instead the SysTick handler, after bumping
//! the counter, calls [`Queue::next_expiration`], which dequeues every timer whose
//! deadline has passed and wakes its waker. Waking an embassy task runs the
//! executor's pender (which issues `SEV`), so the parked executor wakes and
//! re-polls. The result is that timers are serviced on the first SysTick at or
//! after their deadline — a wake granularity of one SysTick period.
//!
//! # Why a 64-bit counter lives behind the critical section
//!
//! ARMv7-M has no 64-bit atomic instructions, so the monotonic counter cannot be
//! an `AtomicU64`. It instead lives in the same `critical_section::Mutex` as the
//! queue, as a `Cell<u64>`; reads and the handler's increment are single-core
//! critical sections (a PRIMASK toggle), which is cheap and race-free here.

use core::cell::{Cell, RefCell};

use cortex_m::peripheral::{SYST, syst::SystClkSource};
use critical_section::Mutex;
use embassy_time_driver::Driver;
use embassy_time_queue_utils::Queue;

/// SysTick reload value (counts down from this to zero, firing on the underflow).
///
/// The AN386 model clocks SysTick from the (emulated) processor clock, so a
/// reload of `N` produces an interrupt every `N + 1` core cycles of *virtual*
/// time. A modest reload keeps timer-wake latency low; the exact wall-clock rate
/// does not matter (see the module docs), only that ticks accrue while the
/// executor sleeps.
const SYSTICK_RELOAD: u32 = 6_000;

/// How many [`embassy_time_driver::TICK_HZ`] ticks one SysTick period represents.
///
/// The timebase is fixed at 1 MHz (`tick-hz-1_000_000`), so `now()` is reported
/// in microseconds. Each SysTick advances that virtual microsecond clock by this
/// many ticks; it sets how fast virtual time runs and therefore how many SysTick
/// interrupts a given protocol interval costs. 1000 µs per SysTick keeps the
/// emulated run short (a 10 s protocol deadline is ~10_000 SysTicks) while still
/// leaving sub-deadline wake granularity.
const TICKS_PER_SYSTICK: u64 = 1_000;

/// The SysTick-backed time driver: the monotonic tick counter plus the software
/// timer queue, both guarded by one critical-section mutex. The SysTick handler
/// owns advancing the counter and draining the queue; [`Driver::now`] reads the
/// counter and [`Driver::schedule_wake`] enqueues.
struct SystickDriver {
  inner: Mutex<Inner>,
}

/// The driver's mutable state behind the critical-section mutex.
struct Inner {
  /// Monotonic virtual time in `TICK_HZ` ticks, advanced by the SysTick handler.
  now: Cell<u64>,
  /// The 8-slot timer queue holding scheduled wakers.
  queue: RefCell<Queue>,
}

embassy_time_driver::time_driver_impl!(static DRIVER: SystickDriver = SystickDriver {
  inner: Mutex::new(Inner {
    now: Cell::new(0),
    queue: RefCell::new(Queue::new()),
  }),
});

impl Driver for SystickDriver {
  fn now(&self) -> u64 {
    critical_section::with(|cs| self.inner.borrow(cs).now.get())
  }

  fn schedule_wake(&self, at: u64, waker: &core::task::Waker) {
    critical_section::with(|cs| {
      let inner = self.inner.borrow(cs);
      // Enqueue (or advance) this waker's deadline. There is no hardware alarm to
      // (re)arm: the free-running SysTick handler drains the queue every tick, so
      // a newly scheduled timer is picked up on the next SysTick at or after `at`.
      // `next_expiration` is still called when the queue changed so a timer whose
      // deadline is already in the past fires immediately rather than waiting a
      // tick.
      let mut queue = inner.queue.borrow_mut();
      if queue.schedule_wake(at, waker) {
        queue.next_expiration(inner.now.get());
      }
    });
  }
}

/// Start the SysTick time driver: configure and enable the SysTick exception.
///
/// Call once, early in `main`, before the executor starts. Consumes the [`SYST`]
/// peripheral so nothing else can reprogram it.
pub fn init(mut syst: SYST) {
  // Clock SysTick from the processor clock and fire on underflow.
  syst.set_clock_source(SystClkSource::Core);
  syst.set_reload(SYSTICK_RELOAD);
  syst.clear_current();
  syst.enable_interrupt();
  syst.enable_counter();
  // Keep the peripheral handle from dropping the counter's enable on scope exit:
  // dropping `SYST` does not stop the timer, but moving it out of `init` would
  // let a later `take()` of the peripherals hand it out again. Forgetting it pins
  // ownership here for the program's life.
  core::mem::forget(syst);
}

/// The SysTick exception handler: advance the virtual clock and dispatch any
/// timers that have now expired.
///
/// `cortex-m-rt` routes the SysTick exception here by the reserved name
/// `SysTick`. It runs in interrupt context; the state access takes the same
/// `critical_section` the driver uses, and `next_expiration` waking a task runs
/// the executor pender (`SEV`), resuming the parked executor.
#[cortex_m_rt::exception]
fn SysTick() {
  critical_section::with(|cs| {
    let inner = DRIVER.inner.borrow(cs);
    let now = inner.now.get().wrapping_add(TICKS_PER_SYSTICK);
    inner.now.set(now);
    // Wake every timer whose deadline is at or before the new `now`; the returned
    // next deadline is unused because the handler re-checks every tick rather than
    // programming a one-shot alarm.
    // Ignoring the next-expiration return: SysTick free-runs and re-drains every
    // tick, so there is no alarm register to reprogram from it.
    let _ = inner.queue.borrow_mut().next_expiration(now);
  });
}
