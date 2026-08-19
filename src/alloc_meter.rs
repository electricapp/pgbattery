//! Test-only allocation counter, so a hot path can state its budget as a
//! number and fail when it grows. A stray `format!` in a per-frame renderer
//! is otherwise invisible to a normal test suite.
//!
//! The counter is thread-local and only tallies while [`measure`] is on the
//! stack, so `cargo test`'s parallel threads cannot contaminate each other.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// Allocation count for the thread currently inside [`measure`].
    static COUNT: Cell<u64> = const { Cell::new(0) };
    /// Bytes requested for the thread currently inside [`measure`].
    static BYTES: Cell<u64> = const { Cell::new(0) };
    /// Whether this thread is measuring. Both cells are plain `Cell`s with no
    /// destructor, so TLS access from inside the allocator cannot re-enter it.
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
}

/// A `System` allocator that tallies allocations on measuring threads.
pub(crate) struct CountingAllocator;

impl CountingAllocator {
    fn record(size: usize) {
        if ACTIVE.get() {
            COUNT.set(COUNT.get() + 1);
            BYTES.set(BYTES.get() + size as u64);
        }
    }
}

#[allow(
    unsafe_code,
    reason = "GlobalAlloc is an unsafe trait; every method forwards its \
              contract unchanged to System and only adds a thread-local tally"
)]
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Counted pessimistically: the allocator does not say whether it grew
        // in place or moved, and a moving realloc is a fresh allocation.
        Self::record(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// What [`measure`] observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Allocations {
    /// Number of `alloc`/`alloc_zeroed`/`realloc` calls.
    pub(crate) count: u64,
    /// Total bytes requested across those calls.
    pub(crate) bytes: u64,
}

/// Count the allocations `f` performs on this thread.
///
/// Nesting is not supported and not needed; an inner call would reset the
/// outer tally, so keep exactly one on the stack.
pub(crate) fn measure<T>(f: impl FnOnce() -> T) -> (T, Allocations) {
    COUNT.set(0);
    BYTES.set(0);
    ACTIVE.set(true);
    let value = f();
    ACTIVE.set(false);
    let stats = Allocations {
        count: COUNT.get(),
        bytes: BYTES.get(),
    };
    (value, stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_a_known_allocation() {
        let (_, stats) = measure(|| String::with_capacity(1024));
        assert_eq!(stats.count, 1);
        assert!(stats.bytes >= 1024);
    }

    #[test]
    fn counts_nothing_for_allocation_free_work() {
        let (sum, stats) = measure(|| (0..100u64).sum::<u64>());
        assert_eq!(sum, 4950);
        assert_eq!(stats.count, 0, "arithmetic must not allocate");
    }

    #[test]
    fn stops_counting_after_measure_returns() {
        let ((), stats) = measure(|| ());
        assert_eq!(stats.count, 0);
        // Allocating outside `measure` must not be attributed to anything.
        let leaked = String::from("not counted");
        assert_eq!(leaked.len(), 11);
        let ((), again) = measure(|| ());
        assert_eq!(again.count, 0);
    }
}
