//! Fuzz target for `parse_lsn`.
//!
//! Verifies that `parse_lsn` never panics or hangs on arbitrary byte input.
//! Run with: cargo fuzz run parse_lsn
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        // Must never panic, abort, or infinite-loop regardless of input.
        let _ = pgbattery::governor::parse_lsn(s);
    }
});
