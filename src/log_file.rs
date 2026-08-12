//! `--log-file`: the same lines that go to stderr, appended to a file, so a
//! relay started detached leaves something to read afterwards.
//!
//! Size-capped by rotation rather than by a dependency: `tracing-appender`
//! rotates on a *time* schedule, which bounds nothing. One rotated file is
//! kept, so the pair on disk is bounded at twice the threshold.

use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, Result};
use tracing_subscriber::fmt::MakeWriter;

/// The log records request paths and model names — not secret, but not the rest
/// of the machine's business either (`capture`'s fixtures, same reasoning).
const FILE_MODE: u32 = 0o600;

/// Per file, so the live log plus its one rotation is bounded at ~20 MB.
pub const DEFAULT_MAX_BYTES: u64 = 10 * 1024 * 1024;

/// A `MakeWriter` over a single shared append handle.
#[derive(Clone)]
pub struct LogFile {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    path: PathBuf,
    file: File,
    /// Tracked across writes rather than `stat`-ed per line: the threshold
    /// check runs on every log record.
    size: u64,
    max_bytes: u64,
}

impl LogFile {
    /// Opening fails loudly — the user asked for this file explicitly, so
    /// silently logging nowhere is the wrong answer. Every failure *after*
    /// startup is swallowed instead; see the `Write` impl.
    pub fn open(path: &Path, max_bytes: u64) -> Result<Self> {
        let file = open_append(path)
            .with_context(|| format!("failed to open --log-file: {}", path.display()))?;
        let size = file.metadata().map_or(0, |meta| meta.len());
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                path: path.to_path_buf(),
                file,
                size,
                max_bytes,
            })),
        })
    }
}

impl Write for LogFile {
    /// Always reports success. A logging failure must never reach the request
    /// that produced the line, and `tracing`'s writer path discards the error
    /// anyway — so a failed write drops that one line and the relay carries on.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        guard(&self.inner).append(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let _ = guard(&self.inner).file.flush();
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for LogFile {
    type Writer = LogFile;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl Inner {
    /// `tracing`'s fmt layer writes one whole formatted event per call, so
    /// rotating before the write — never mid-buffer — keeps records intact
    /// across the boundary.
    fn append(&mut self, buf: &[u8]) {
        let len = buf.len() as u64;
        // `size > 0`: a single record larger than the threshold still gets
        // written, rather than rotating an empty file once per line.
        if self.size > 0 && self.size + len > self.max_bytes {
            // A rotation that failed leaves the live file in place; keep
            // appending to it rather than dropping the line.
            let _ = self.rotate();
        }
        match self.file.write_all(buf) {
            Ok(()) => self.size += len,
            // A partial write leaves `size` short of the truth, which would
            // uncap the file; the handle's own length is authoritative.
            Err(_) => self.size = self.file.metadata().map_or(self.size, |meta| meta.len()),
        }
    }

    fn rotate(&mut self) -> io::Result<()> {
        // `rename` replaces an existing `.1` — that replacement is what bounds
        // the pair at two files. A live file that has gone missing (moved by
        // something else, or a previous rotation whose reopen failed) is not an
        // error: reopening below is the recovery either way.
        match fs::rename(&self.path, rotated_path(&self.path)) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        self.file = open_append(&self.path)?;
        self.size = 0;
        Ok(())
    }
}

/// A suffix, not a replaced extension: `relay.log` rotates to `relay.log.1`.
fn rotated_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".1");
    PathBuf::from(name)
}

/// `mode` applies only when this call creates the file; an existing log keeps
/// whatever permissions it already has.
fn open_append(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .append(true)
        .create(true)
        .mode(FILE_MODE)
        .open(path)
}

/// A poisoned lock is not a reason to stop logging: nothing under this mutex
/// panics, and if something ever did, taking the relay's every later log line
/// down with it is worse than reusing the handle.
fn guard(inner: &Mutex<Inner>) -> MutexGuard<'_, Inner> {
    inner
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "relay-{label}-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before epoch")
                .as_nanos()
        ))
    }

    fn write_line(log: &LogFile, line: &str) {
        log.make_writer()
            .write_all(line.as_bytes())
            .expect("the writer never reports failure");
    }

    /// Every file the log has produced. The unique name makes a prefix match
    /// over the temp dir exact, and counting them is the direct statement of
    /// the bound: two files, whatever they are named.
    fn produced_files(path: &Path) -> Vec<PathBuf> {
        let prefix = path
            .file_name()
            .expect("temp path has a file name")
            .as_encoded_bytes()
            .to_vec();
        let mut found: Vec<_> = fs::read_dir(path.parent().expect("temp path has a parent"))
            .expect("temp dir should be readable")
            .flatten()
            .map(|entry| entry.path())
            .filter(|found| {
                found
                    .file_name()
                    .is_some_and(|name| name.as_encoded_bytes().starts_with(&prefix))
            })
            .collect();
        found.sort();
        found
    }

    /// Driven with a 16-byte threshold rather than 10 MB. Two rotations, so the
    /// second one has an existing `.1` to replace: total on disk stays two
    /// files, and the oldest line is gone rather than accumulated into a `.2`.
    #[test]
    fn rotation_at_the_threshold_keeps_exactly_one_old_file() {
        let path = temp_path("log-rotation");
        let rotated = rotated_path(&path);
        let log = LogFile::open(&path, 16).expect("failed to open log file");

        write_line(&log, "first\n");
        assert!(
            !rotated.exists(),
            "nothing may rotate before the threshold is crossed"
        );

        // 6 bytes on disk, 12 more coming: 18 > 16, so this line rotates first.
        write_line(&log, "second-line\n");
        assert_eq!(
            fs::read_to_string(&rotated).expect("the rotated file must exist"),
            "first\n"
        );
        assert_eq!(
            fs::read_to_string(&path).expect("a new live file must exist"),
            "second-line\n"
        );

        write_line(&log, "third-line!\n");
        assert_eq!(
            fs::read_to_string(&rotated).expect("the rotated file must still exist"),
            "second-line\n",
            "the second rotation must replace `.1`, not keep the older one"
        );
        assert_eq!(
            fs::read_to_string(&path).expect("a new live file must exist"),
            "third-line!\n"
        );
        let produced = produced_files(&path);
        assert_eq!(
            produced,
            vec![path.clone(), rotated.clone()],
            "two rotations must leave two files, not accumulate"
        );

        let mode = fs::metadata(&path)
            .expect("live log must exist")
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o077,
            0,
            "log file is group/world accessible: {mode:o}"
        );

        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&rotated);
    }
}
