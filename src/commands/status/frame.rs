//! The dashboard's output buffer: one allocation, repainted in place.

use anyhow::Result;

/// A frame of terminal output, built in full before any of it is written.
///
/// Buffering the whole frame and repainting over the previous one keeps the
/// pane readable for the entire scrape, which can run to the full timeout when
/// a node is down. Erasing first would leave it blank for that window.
pub(super) struct Frame {
    buf: String,
    /// Ends every line; erases the row's tail when repainting in place.
    line_end: &'static str,
    /// Precedes the first line of a repaint.
    home: &'static str,
    /// Clears rows the previous frame used and this one does not.
    clear_below: &'static str,
}

/// Capacity the buffer starts at, so the first frame does not grow it either.
pub(super) const INITIAL_CAPACITY: usize = 4096;

impl Frame {
    pub(super) fn new(repaint_in_place: bool) -> Self {
        Self {
            buf: String::with_capacity(INITIAL_CAPACITY),
            line_end: if repaint_in_place { "\x1b[K\n" } else { "\n" },
            home: if repaint_in_place { "\x1b[H" } else { "" },
            clear_below: if repaint_in_place { "\x1b[J" } else { "" },
        }
    }

    /// Start a frame, keeping the buffer's capacity.
    pub(super) fn begin(&mut self) {
        self.buf.clear();
        self.buf.push_str(self.home);
    }

    /// Terminate the current line.
    pub(super) fn endl(&mut self) {
        self.buf.push_str(self.line_end);
    }

    /// Append a literal without going through the formatting machinery.
    pub(super) fn push_str(&mut self, s: &str) {
        self.buf.push_str(s);
    }

    /// Close the frame, clearing whatever the previous one left below it.
    pub(super) fn end(&mut self) {
        self.buf.push_str(self.clear_below);
    }

    /// The frame as written so far. Renderers write; only tests read back.
    #[cfg(test)]
    pub(super) fn as_str(&self) -> &str {
        &self.buf
    }

    /// Write the whole frame with one syscall, rather than one per line.
    pub(super) fn flush(&mut self) -> Result<()> {
        self.end();
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        self.write_to(&mut out)
    }

    /// Emit the buffer as it stands. `flush` closes the frame first; this is
    /// the raw write, so a test can supply its own sink.
    fn write_to(&self, out: &mut impl std::io::Write) -> Result<()> {
        out.write_all(self.buf.as_bytes())?;
        out.flush()?;
        Ok(())
    }
}

impl std::fmt::Write for Frame {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.buf.push_str(s);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc_meter::measure;
    use std::fmt::Write as _;

    #[test]
    fn in_place_frames_carry_the_repaint_sequences() {
        let mut frame = Frame::new(true);
        frame.begin();
        write!(&mut frame, "row").ok();
        frame.endl();
        frame.end();
        assert_eq!(frame.as_str(), "\x1b[Hrow\x1b[K\n\x1b[J");
    }

    #[test]
    fn piped_frames_carry_none_of_them() {
        let mut frame = Frame::new(false);
        frame.begin();
        write!(&mut frame, "row").ok();
        frame.endl();
        frame.end();
        assert_eq!(frame.as_str(), "row\n");
    }

    /// Counts `write` calls reaching the underlying descriptor.
    struct CountingWriter {
        writes: usize,
        sunk: usize,
    }

    impl std::io::Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.writes += 1;
            self.sunk += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A frame leaves in one `write`, whatever its line count. Writing rows
    /// individually through a line-buffered stdout costs one syscall per line.
    #[test]
    fn a_frame_is_a_single_write() {
        let mut frame = Frame::new(true);
        frame.begin();
        for _ in 0..12 {
            write!(&mut frame, "a row").ok();
            frame.endl();
        }

        let mut counter = CountingWriter { writes: 0, sunk: 0 };
        frame.write_to(&mut counter).ok();
        assert_eq!(counter.writes, 1, "a frame must be one write");
        assert_eq!(counter.sunk, frame.as_str().len());
        assert_eq!(
            frame.as_str().matches('\n').count(),
            12,
            "twelve lines went out in that one write"
        );
    }

    /// `begin` reuses the buffer rather than reallocating it, which is what
    /// makes a steady-state repaint allocation-free.
    #[test]
    fn begin_reuses_the_buffer() {
        // Built outside `measure`, so the tally covers only the refill.
        let old = "x".repeat(512);
        let new = "y".repeat(512);

        let mut frame = Frame::new(true);
        frame.begin();
        write!(&mut frame, "{old}").ok();

        let ((), stats) = measure(|| {
            frame.begin();
            write!(&mut frame, "{new}").ok();
        });
        assert_eq!(stats.count, 0, "refilling the buffer must not allocate");
        assert!(frame.as_str().ends_with('y'));
        assert!(!frame.as_str().contains('x'), "stale bytes survived begin");
    }
}
