//! Time-source abstraction.
//!
//! [`LeaseState`](crate) reads time through [`Clock`] rather than calling
//! `Instant::now()` directly. Production wires [`SystemClock`]; tests can
//! supply a manually-advanced clock so lease expiry can be exercised
//! without `thread::sleep`.

use std::time::Instant;

/// Monotonic clock. The only method we need is `now()` — duration
/// arithmetic on the returned `Instant` is provided by `std`.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Instant;
}

/// Production clock — forwards directly to [`Instant::now`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    #[inline]
    fn now(&self) -> Instant {
        Instant::now()
    }
}
