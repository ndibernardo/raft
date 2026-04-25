use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::storage::Storage;
use crate::types::{LogEntry, LogIndex, NodeId, Term};

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
    log: Vec<LogEntry<Cmd>>,
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
            log,
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
        for entry in &self.log {
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

    fn current_term(&self) -> Result<Term, Self::Error> {
        Ok(self.current_term)
    }

    fn set_current_term(&mut self, term: Term) -> Result<(), Self::Error> {
        self.current_term = term;
        self.flush_meta()
    }

    fn voted_for(&self) -> Result<Option<NodeId>, Self::Error> {
        Ok(self.voted_for)
    }

    fn set_voted_for(&mut self, candidate: Option<NodeId>) -> Result<(), Self::Error> {
        self.voted_for = candidate;
        self.flush_meta()
    }

    fn last_log_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(LogIndex::from_length(self.log.len()))
    }

    fn term_at(&self, index: LogIndex) -> Result<Option<Term>, Self::Error> {
        match index.to_array_index() {
            None => Ok(Some(Term::default())),
            Some(idx) => Ok(self.log.get(idx).map(|e| e.term)),
        }
    }

    fn entry(&self, index: LogIndex) -> Result<Option<LogEntry<Cmd>>, Self::Error> {
        match index.to_array_index() {
            None => Ok(None),
            Some(idx) => Ok(self.log.get(idx).cloned()),
        }
    }

    fn entries_from(&self, start: LogIndex) -> Result<Vec<LogEntry<Cmd>>, Self::Error> {
        match start.to_array_index() {
            None => Ok(self.log.clone()),
            Some(idx) => Ok(self.log.get(idx..).unwrap_or_default().to_vec()),
        }
    }

    fn append(&mut self, entry: LogEntry<Cmd>) -> Result<LogIndex, Self::Error> {
        self.append_to_log_file(&entry)?;
        self.log.push(entry);
        Ok(LogIndex::from_length(self.log.len()))
    }

    fn truncate_from(&mut self, index: LogIndex) -> Result<(), Self::Error> {
        if let Some(idx) = index.to_array_index() {
            self.log.truncate(idx);
        }
        self.rewrite_log_file()
    }

    /// Append entries per Raft rules (§5.3): if an existing entry conflicts with
    /// a new one (same index, different term), delete it and everything after.
    /// On a conflict the whole log is rewritten atomically; on a pure append
    /// each new entry is fsynced individually, keeping the common path fast.
    fn append_entries(
        &mut self,
        prev_log_index: LogIndex,
        entries: Vec<LogEntry<Cmd>>,
    ) -> Result<(), Self::Error> {
        let mut insert_index = prev_log_index.next();
        let mut conflict_found = false;

        for entry in entries {
            match insert_index.to_array_index() {
                Some(idx) if idx < self.log.len() => {
                    if self.log[idx].term != entry.term {
                        self.log.truncate(idx);
                        self.log.push(entry);
                        conflict_found = true;
                    }
                    // Same term at this index: already present, skip.
                }
                _ => {
                    if !conflict_found {
                        // Pure append path: persist incrementally.
                        self.append_to_log_file(&entry)?;
                    }
                    self.log.push(entry);
                }
            }
            insert_index = insert_index.next();
        }

        if conflict_found {
            self.rewrite_log_file()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Term;

    fn open_fresh(dir: &Path) -> FileStorage<String> {
        FileStorage::open(dir).expect("open failed")
    }

    #[test]
    fn meta_survives_reopen() {
        let tmp = tempfile::tempdir().expect("tempdir");
        {
            let mut s = open_fresh(tmp.path());
            s.set_current_term(Term::from(7)).expect("set term");
            s.set_voted_for(Some(NodeId::from(2))).expect("set vote");
        }
        let s = open_fresh(tmp.path());
        assert_eq!(s.current_term().expect("term"), Term::from(7));
        assert_eq!(s.voted_for().expect("vote"), Some(NodeId::from(2)));
    }

    #[test]
    fn log_survives_reopen() {
        let tmp = tempfile::tempdir().expect("tempdir");
        {
            let mut s = open_fresh(tmp.path());
            s.append(LogEntry { term: Term::from(1), command: Some("a".into()) })
                .expect("append");
            s.append(LogEntry { term: Term::from(1), command: Some("b".into()) })
                .expect("append");
        }
        let s = open_fresh(tmp.path());
        assert_eq!(s.last_log_index().expect("idx"), LogIndex::from(2));
        assert_eq!(
            s.entry(LogIndex::from(1)).expect("entry").map(|e| e.command),
            Some(Some("a".into()))
        );
        assert_eq!(
            s.entry(LogIndex::from(2)).expect("entry").map(|e| e.command),
            Some(Some("b".into()))
        );
    }

    #[test]
    fn truncate_survives_reopen() {
        let tmp = tempfile::tempdir().expect("tempdir");
        {
            let mut s = open_fresh(tmp.path());
            for ch in ["a", "b", "c"] {
                s.append(LogEntry { term: Term::from(1), command: Some(ch.into()) })
                    .expect("append");
            }
            s.truncate_from(LogIndex::from(2)).expect("truncate");
        }
        let s = open_fresh(tmp.path());
        assert_eq!(s.last_log_index().expect("idx"), LogIndex::from(1));
        assert_eq!(
            s.entry(LogIndex::from(1)).expect("entry").map(|e| e.command),
            Some(Some("a".into()))
        );
    }

    #[test]
    fn conflict_replaces_entries_and_survives_reopen() {
        let tmp = tempfile::tempdir().expect("tempdir");
        {
            let mut s = open_fresh(tmp.path());
            s.append(LogEntry { term: Term::from(1), command: Some("a".into()) })
                .expect("append");
            s.append(LogEntry { term: Term::from(1), command: Some("old".into()) })
                .expect("append");
            // Entry at index 2 conflicts (term 2 vs 1): truncate and replace.
            s.append_entries(
                LogIndex::from(1),
                vec![LogEntry { term: Term::from(2), command: Some("new".into()) }],
            )
            .expect("append_entries");
        }
        let s = open_fresh(tmp.path());
        assert_eq!(s.last_log_index().expect("idx"), LogIndex::from(2));
        assert_eq!(
            s.entry(LogIndex::from(2)).expect("entry").map(|e| e.command),
            Some(Some("new".into()))
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
            s.append(LogEntry { term: Term::from(1), command: None })
                .expect("append noop");
        }
        let s: FileStorage<String> = open_fresh(tmp.path());
        assert_eq!(
            s.entry(LogIndex::from(1)).expect("entry").map(|e| e.command),
            Some(None)
        );
    }
}
