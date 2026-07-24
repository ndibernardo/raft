use std::fs::File;
use std::fs::OpenOptions;
use std::fs::{self};
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::io::{self};
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use crate::core::types::Log;
use crate::core::types::LogEntry;
use crate::core::types::LogIndex;
use crate::core::types::NodeId;
use crate::core::types::Snapshot;
use crate::core::types::Term;
use crate::storage::LoadedState;
use crate::storage::Storage;

/// Error type for FileStorage operations.
#[derive(Debug, thiserror::Error)]
pub enum FileStorageError {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    #[error("corrupt storage: {0}")]
    Corrupt(#[from] serde_json::Error),
}

#[derive(Serialize, Deserialize)]
struct Meta {
    current_term: Term,
    voted_for: Option<NodeId>,
}

/// Line 1 of `log.jsonl` once it has been rewritten at least once (by
/// `truncate_from` or `install_snapshot`): the index of the entry on line 2.
/// Distinguishable from a `LogEntry` line by its distinct field name, so a
/// pure-append-only file (never rewritten) parses with no header at all.
#[derive(Serialize, Deserialize)]
struct LogFileHeader {
    first_index: LogIndex,
}

/// Disk-backed storage. Persistent state lives in up to three files inside `dir`:
///   meta.json      — current term and voted_for, written atomically via rename
///   snapshot.json  — most recent snapshot (meta + opaque state machine bytes), if any
///   log.jsonl      — one JSON object per log entry, one entry per line, optionally
///                    preceded by a `{"first_index": N}` header line
///
/// The in-memory log acts as a write-through cache: reads are served from
/// memory, writes update memory then flush to disk with fsync before returning.
/// This satisfies the durability requirement of §5.1 (respond only after
/// persisting state).
pub struct FileStorage<Cmd> {
    dir: PathBuf,
    current_term: Term,
    voted_for: Option<NodeId>,
    snapshot: Option<Snapshot>,
    log: Log<Cmd>,
}

impl<Cmd> FileStorage<Cmd>
where
    Cmd: Serialize + for<'de> Deserialize<'de>,
{
    /// Open (or create) storage rooted at `dir`. On first use the directory
    /// is created and all files start empty (term=0, no vote, no snapshot, empty log).
    ///
    /// Reconciles a crash that landed between `install_snapshot`'s two durable
    /// writes: if `log.jsonl`'s header claims entries starting at or before the
    /// snapshot boundary, the overlapping prefix is dropped in memory (the
    /// on-disk log is only rewritten again on the next mutation).
    pub fn open(dir: &Path) -> Result<Self, FileStorageError> {
        fs::create_dir_all(dir)?;
        let meta = Self::read_meta(dir)?;
        let snapshot = Self::read_snapshot(dir)?;
        let (claimed_first_index, entries) = Self::read_log(dir)?;

        let log = match &snapshot {
            Some(snap) => {
                let surviving = entries
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| {
                        claimed_first_index.advance_by(*i as u64) > snap.meta.last_index
                    })
                    .map(|(_, entry)| entry)
                    .collect();
                Log::from_snapshot_and_suffix(snap.meta.last_index, snap.meta.last_term, surviving)
            }
            None => Log::from_entries(entries),
        };

        Ok(Self {
            dir: dir.to_path_buf(),
            current_term: meta.current_term,
            voted_for: meta.voted_for,
            snapshot,
            log,
        })
    }

    fn meta_path(&self) -> PathBuf {
        self.dir.join("meta.json")
    }

    fn snapshot_path(&self) -> PathBuf {
        self.dir.join("snapshot.json")
    }

    fn log_path(&self) -> PathBuf {
        self.dir.join("log.jsonl")
    }

    fn read_meta(dir: &Path) -> Result<Meta, FileStorageError> {
        let path = dir.join("meta.json");
        if !path.exists() {
            return Ok(Meta {
                current_term: Term::default(),
                voted_for: None,
            });
        }
        let bytes = fs::read(&path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn read_snapshot(dir: &Path) -> Result<Option<Snapshot>, FileStorageError> {
        let path = dir.join("snapshot.json");
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path)?;
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    /// Returns the first index claimed by line 1's header, or `LogIndex::from(1)`
    /// if the file has never been rewritten (no header, or the file is absent).
    fn read_log(dir: &Path) -> Result<(LogIndex, Vec<LogEntry<Cmd>>), FileStorageError> {
        let path = dir.join("log.jsonl");
        if !path.exists() {
            return Ok((LogIndex::from(1), Vec::new()));
        }
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut first_index = LogIndex::from(1);
        let mut entries = Vec::new();

        if let Some(line) = lines.next() {
            let line = line?;
            if !line.is_empty() {
                let value: serde_json::Value = serde_json::from_str(&line)?;
                match serde_json::from_value::<LogFileHeader>(value.clone()) {
                    Ok(header) => first_index = header.first_index,
                    Err(_) => entries.push(serde_json::from_value(value)?),
                }
            }
        }

        for line in lines {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            entries.push(serde_json::from_str(&line)?);
        }

        Ok((first_index, entries))
    }

    /// Atomically overwrite meta.json: write temp file → fsync → rename → fsync dir.
    fn flush_meta(&self) -> Result<(), FileStorageError> {
        let tmp = self.dir.join("meta.json.tmp");
        let meta = Meta {
            current_term: self.current_term,
            voted_for: self.voted_for,
        };
        let bytes = serde_json::to_vec(&meta)?;
        let mut file = File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, self.meta_path())?;
        // Fsync the directory so the rename is visible after a crash.
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    /// Atomically overwrite snapshot.json: write temp file → fsync → rename → fsync dir.
    fn flush_snapshot(&self, snapshot: &Snapshot) -> Result<(), FileStorageError> {
        let tmp = self.dir.join("snapshot.json.tmp");
        let bytes = serde_json::to_vec(snapshot)?;
        let mut file = File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, self.snapshot_path())?;
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    /// Append one serialised entry to log.jsonl and fsync.
    fn append_to_log_file(&self, entry: &LogEntry<Cmd>) -> Result<(), FileStorageError> {
        let mut line = serde_json::to_string(entry)?;
        line.push('\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path())?;
        file.write_all(line.as_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    /// Rewrite log.jsonl from the in-memory cache atomically and fsync. Always
    /// leads with a `{"first_index": N}` header so a reload can tell how far a
    /// stale (pre-rewrite) file's entries reach relative to the snapshot boundary.
    fn rewrite_log_file(&self) -> Result<(), FileStorageError> {
        let tmp = self.dir.join("log.jsonl.tmp");
        let mut file = File::create(&tmp)?;
        let header = LogFileHeader {
            first_index: self.log.first_index(),
        };
        let mut header_line = serde_json::to_string(&header)?;
        header_line.push('\n');
        file.write_all(header_line.as_bytes())?;
        for entry in self.log.iter() {
            let mut line = serde_json::to_string(entry)?;
            line.push('\n');
            file.write_all(line.as_bytes())?;
        }
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, self.log_path())?;
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }
}

impl<Cmd> Storage<Cmd> for FileStorage<Cmd>
where
    Cmd: Clone + Serialize + for<'de> Deserialize<'de>,
{
    type Error = FileStorageError;

    fn load(&self) -> Result<LoadedState<Cmd>, Self::Error> {
        Ok(LoadedState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            snapshot: self.snapshot.clone(),
            entries: self.log.iter().cloned().collect(),
        })
    }

    /// Single `flush_meta` call — one fsync for both fields, not two. `Node` only
    /// calls this when term or vote actually changed, so a steady-state heartbeat
    /// with nothing to persist costs zero fsyncs.
    fn set_meta(&mut self, term: Term, voted_for: Option<NodeId>) -> Result<(), Self::Error> {
        self.current_term = term;
        self.voted_for = voted_for;
        self.flush_meta()
    }

    fn truncate_from(&mut self, index: LogIndex) -> Result<(), Self::Error> {
        self.log.truncate_from(index);
        self.rewrite_log_file()
    }

    /// `Node` already resolved any conflict in memory and hands us exactly the
    /// suffix to persist — append each entry individually so a crash mid-batch
    /// leaves the file with a valid (if partial) prefix rather than none of it.
    fn append(&mut self, entries: &[LogEntry<Cmd>]) -> Result<(), Self::Error> {
        for entry in entries {
            self.append_to_log_file(entry)?;
            self.log.append(entry.clone());
        }
        Ok(())
    }

    /// Snapshot durable first, then the log prefix is dropped and rewritten —
    /// a crash between the two is reconciled by `open()` on the next start.
    fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error> {
        self.flush_snapshot(snapshot)?;
        self.snapshot = Some(snapshot.clone());
        self.log
            .compact_through(snapshot.meta.last_index, snapshot.meta.last_term);
        self.rewrite_log_file()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::LogPayload;
    use crate::core::types::Term;

    fn open_fresh(dir: &Path) -> FileStorage<String> {
        FileStorage::open(dir).expect("open failed")
    }

    #[test]
    fn meta_survives_reopen() {
        let tmp = tempfile::tempdir().expect("tempdir");
        {
            let mut s = open_fresh(tmp.path());
            s.set_meta(Term::from(7), Some(NodeId::from(2)))
                .expect("set meta");
        }
        let s = open_fresh(tmp.path());
        let loaded = s.load().expect("load");
        assert_eq!(loaded.current_term, Term::from(7));
        assert_eq!(loaded.voted_for, Some(NodeId::from(2)));
    }

    #[test]
    fn log_survives_reopen() {
        let tmp = tempfile::tempdir().expect("tempdir");
        {
            let mut s = open_fresh(tmp.path());
            s.append(&[
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("SET name=miles".into()),
                },
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("SET counter=1".into()),
                },
            ])
            .expect("append");
        }
        let s = open_fresh(tmp.path());
        let entries = s.load().expect("load").entries;
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].payload,
            LogPayload::Command("SET name=miles".into())
        );
        assert_eq!(
            entries[1].payload,
            LogPayload::Command("SET counter=1".into())
        );
    }

    #[test]
    fn truncate_survives_reopen() {
        let tmp = tempfile::tempdir().expect("tempdir");
        {
            let mut s = open_fresh(tmp.path());
            let entries: Vec<LogEntry<String>> =
                ["SET name=miles", "SET counter=1", "SET price=100"]
                    .into_iter()
                    .map(|cmd| LogEntry {
                        term: Term::from(1),
                        payload: LogPayload::Command(cmd.into()),
                    })
                    .collect();
            s.append(&entries).expect("append");
            s.truncate_from(LogIndex::from(2)).expect("truncate");
        }
        let s = open_fresh(tmp.path());
        let entries = s.load().expect("load").entries;
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].payload,
            LogPayload::Command("SET name=miles".into())
        );
    }

    #[test]
    fn truncate_then_append_replaces_entries_and_survives_reopen() {
        let tmp = tempfile::tempdir().expect("tempdir");
        {
            let mut s = open_fresh(tmp.path());
            s.append(&[
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("SET name=miles".into()),
                },
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("SET status=pending".into()),
                },
            ])
            .expect("append");
            // Entry at index 2 conflicts (term 2 vs 1): Node resolves the conflict
            // in memory and tells storage to truncate from there, then append the
            // replacement suffix — storage never has to detect the conflict itself.
            s.truncate_from(LogIndex::from(2)).expect("truncate");
            s.append(&[LogEntry {
                term: Term::from(2),
                payload: LogPayload::Command("SET status=active".into()),
            }])
            .expect("append");
        }
        let s = open_fresh(tmp.path());
        let entries = s.load().expect("load").entries;
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[1].payload,
            LogPayload::Command("SET status=active".into())
        );
    }

    #[test]
    fn corrupt_log_file_returns_corrupt_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::write(tmp.path().join("log.jsonl"), b"not valid json\n")
            .expect("write corrupt log");

        let result = FileStorage::<String>::open(tmp.path());

        assert!(matches!(result, Err(FileStorageError::Corrupt(_))));
    }

    #[test]
    fn noop_entry_round_trips() {
        let tmp = tempfile::tempdir().expect("tempdir");
        {
            let mut s: FileStorage<String> = open_fresh(tmp.path());
            s.append(&[LogEntry {
                term: Term::from(1),
                payload: LogPayload::NoOp,
            }])
            .expect("append noop");
        }
        let s: FileStorage<String> = open_fresh(tmp.path());
        let entries = s.load().expect("load").entries;
        assert_eq!(entries[0].payload, LogPayload::NoOp);
    }

    fn test_snapshot(last_index: u64, last_term: u64, bytes: Vec<u8>) -> Snapshot {
        use std::collections::HashMap;

        use crate::core::types::ClusterConfig;
        use crate::core::types::SnapshotData;
        use crate::core::types::SnapshotMeta;

        let members = HashMap::from([(NodeId::from(1), "127.0.0.1:9001".parse().unwrap())]);
        Snapshot {
            meta: SnapshotMeta {
                last_index: LogIndex::from(last_index),
                last_term: Term::from(last_term),
                config: ClusterConfig::new(members).unwrap(),
            },
            data: SnapshotData::new(bytes),
        }
    }

    fn three_entry_log() -> [LogEntry<String>; 3] {
        [
            LogEntry {
                term: Term::from(1),
                payload: LogPayload::Command("SET name=miles".into()),
            },
            LogEntry {
                term: Term::from(1),
                payload: LogPayload::Command("SET status=pending".into()),
            },
            LogEntry {
                term: Term::from(2),
                payload: LogPayload::Command("SET status=active".into()),
            },
        ]
    }

    #[test]
    fn install_snapshot_drops_covered_log_prefix() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut s = open_fresh(tmp.path());
        s.append(&three_entry_log()).expect("append");

        s.install_snapshot(&test_snapshot(2, 1, vec![9, 9]))
            .expect("install snapshot");

        let entries = s.load().expect("load").entries;
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].payload,
            LogPayload::Command("SET status=active".into())
        );
    }

    #[test]
    fn install_snapshot_survives_reopen() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let snapshot = test_snapshot(2, 1, vec![10, 20, 30]);
        {
            let mut s = open_fresh(tmp.path());
            s.append(&three_entry_log()).expect("append");
            s.install_snapshot(&snapshot).expect("install snapshot");
        }

        let s = open_fresh(tmp.path());
        let loaded = s.load().expect("load");
        assert_eq!(loaded.snapshot, Some(snapshot));
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(
            loaded.entries[0].payload,
            LogPayload::Command("SET status=active".into())
        );
    }

    #[test]
    fn reopen_reconciles_log_written_before_snapshot_rewrite() {
        let tmp = tempfile::tempdir().expect("tempdir");
        {
            let mut s = open_fresh(tmp.path());
            // No truncate/install_snapshot yet, so log.jsonl has no header —
            // exactly the state a crash between install_snapshot's two durable
            // writes would leave behind.
            s.append(&three_entry_log()).expect("append");

            let snapshot = test_snapshot(2, 1, vec![7, 7]);
            let bytes = serde_json::to_vec(&snapshot).expect("serialize snapshot");
            std::fs::write(tmp.path().join("snapshot.json"), bytes)
                .expect("write snapshot.json directly");
        }

        let s = open_fresh(tmp.path());
        let loaded = s.load().expect("load");
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(
            loaded.entries[0].payload,
            LogPayload::Command("SET status=active".into())
        );
    }

    #[test]
    fn corrupt_snapshot_file_returns_corrupt_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::write(tmp.path().join("snapshot.json"), b"not valid json")
            .expect("write corrupt snapshot");

        let result = FileStorage::<String>::open(tmp.path());

        assert!(matches!(result, Err(FileStorageError::Corrupt(_))));
    }

    #[test]
    fn log_header_line_round_trips_first_index() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut s = open_fresh(tmp.path());
        s.append(&three_entry_log()[..2]).expect("append");
        s.truncate_from(LogIndex::from(2)).expect("truncate");

        let contents =
            std::fs::read_to_string(tmp.path().join("log.jsonl")).expect("read log file");
        let first_line = contents.lines().next().expect("header line present");
        let header: LogFileHeader = serde_json::from_str(first_line).expect("header parses");

        assert_eq!(header.first_index, LogIndex::from(1));
    }
}
