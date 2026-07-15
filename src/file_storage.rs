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

use crate::storage::LoadedState;
use crate::storage::Storage;
use crate::types::Log;
use crate::types::LogEntry;
use crate::types::LogIndex;
use crate::types::NodeId;
use crate::types::Term;

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

/// Disk-backed storage. Persistent state lives in two files inside `dir`:
///   meta.json  — current term and voted_for, written atomically via rename
///   log.jsonl  — one JSON object per log entry, one entry per line
///
/// The in-memory log acts as a write-through cache: reads are served from
/// memory, writes update memory then flush to disk with fsync before returning.
/// This satisfies the durability requirement of §5.1 (respond only after
/// persisting state).
pub struct FileStorage<Cmd> {
    dir: PathBuf,
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log<Cmd>,
}

impl<Cmd> FileStorage<Cmd>
where
    Cmd: Serialize + for<'de> Deserialize<'de>,
{
    /// Open (or create) storage rooted at `dir`. On first use the directory
    /// is created and both files start empty (term=0, no vote, empty log).
    pub fn open(dir: &Path) -> Result<Self, FileStorageError> {
        fs::create_dir_all(dir)?;
        let meta = Self::read_meta(dir)?;
        let log = Self::read_log(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            current_term: meta.current_term,
            voted_for: meta.voted_for,
            log: Log::from_entries(log),
        })
    }

    fn meta_path(&self) -> PathBuf {
        self.dir.join("meta.json")
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

    fn read_log(dir: &Path) -> Result<Vec<LogEntry<Cmd>>, FileStorageError> {
        let path = dir.join("log.jsonl");
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let entry: LogEntry<Cmd> = serde_json::from_str(&line)?;
            entries.push(entry);
        }
        Ok(entries)
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

    /// Rewrite log.jsonl from the in-memory cache atomically and fsync.
    fn rewrite_log_file(&self) -> Result<(), FileStorageError> {
        let tmp = self.dir.join("log.jsonl.tmp");
        let mut file = File::create(&tmp)?;
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
        Ok((
            self.current_term,
            self.voted_for,
            self.log.iter().cloned().collect(),
        ))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogPayload;
    use crate::types::Term;

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
        let (term, voted_for, _) = s.load().expect("load");
        assert_eq!(term, Term::from(7));
        assert_eq!(voted_for, Some(NodeId::from(2)));
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
        let (_, _, entries) = s.load().expect("load");
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
        let (_, _, entries) = s.load().expect("load");
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
        let (_, _, entries) = s.load().expect("load");
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
        let (_, _, entries) = s.load().expect("load");
        assert_eq!(entries[0].payload, LogPayload::NoOp);
    }
}
