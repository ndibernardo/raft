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
use crate::core::types::SuffixDisposition;
use crate::core::types::Term;
use crate::storage::LoadedState;
use crate::storage::Storage;

/// Why a `FileStorage` operation failed.
#[derive(Debug, thiserror::Error)]
pub enum FileStorageError {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    #[error("corrupt storage: {0}")]
    Corrupt(#[from] serde_json::Error),
}

/// On-disk form of `meta.json`.
#[derive(Serialize, Deserialize)]
struct Meta {
    current_term: Term,
    voted_for: Option<NodeId>,
}

/// First line of `log.jsonl` once the file has been rewritten at least once, by
/// `truncate_from` or `install_snapshot`. It names the index of the entry on the
/// following line.
///
/// Its field name does not occur in a serialized `LogEntry`, so a file that has
/// only ever been appended to parses correctly with no header present.
#[derive(Serialize, Deserialize)]
struct LogFileHeader {
    first_index: LogIndex,
}

/// On-disk form of `SuffixDisposition`.
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DispositionDto {
    Retain,
    Discard,
}

impl From<SuffixDisposition> for DispositionDto {
    fn from(disposition: SuffixDisposition) -> Self {
        match disposition {
            SuffixDisposition::Retain => Self::Retain,
            SuffixDisposition::Discard => Self::Discard,
        }
    }
}

impl From<DispositionDto> for SuffixDisposition {
    fn from(dto: DispositionDto) -> Self {
        match dto {
            DispositionDto::Retain => Self::Retain,
            DispositionDto::Discard => Self::Discard,
        }
    }
}

/// On-disk record of a snapshot install that has started and not yet finished.
///
/// It names the boundary being installed and what was decided about the entries
/// above it. `open` finds it only after a crash, and uses it to finish or roll
/// back the install before serving any read.
#[derive(Serialize, Deserialize)]
struct InstallIntent {
    last_index: LogIndex,
    last_term: Term,
    disposition: DispositionDto,
}

impl InstallIntent {
    fn new(snapshot: &Snapshot, disposition: SuffixDisposition) -> Self {
        Self {
            last_index: snapshot.meta.last_index,
            last_term: snapshot.meta.last_term,
            disposition: disposition.into(),
        }
    }

    /// Whether `snapshot` is the one this record was written for.
    ///
    /// A boundary is identified by index and term together. Two snapshots
    /// sharing both describe the same committed prefix, so either one satisfies
    /// the record.
    fn covers(&self, snapshot: &Snapshot) -> bool {
        snapshot.meta.last_index == self.last_index && snapshot.meta.last_term == self.last_term
    }
}

/// Disk-backed storage. State lives in up to four files inside `dir`.
///
/// `meta.json` holds the current term and vote, replaced atomically by rename.
/// `snapshot.json` holds the most recent snapshot, if any. `log.jsonl` holds one
/// JSON-encoded entry per line, optionally preceded by a `LogFileHeader` line.
/// `install.json` exists only while a snapshot install is in flight.
///
/// The in-memory log is a write-through cache: reads come from memory, and a
/// write updates memory and then reaches disk with an fsync before the call
/// returns. That ordering is what satisfies the durability requirement of
/// section 5.1, which forbids responding to an RPC before the state it
/// acknowledges is persisted.
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
    /// Opens storage rooted at `dir`, creating the directory if it is absent. A
    /// missing file is read as its empty value: term 0, no vote, no snapshot,
    /// no entries.
    ///
    /// Also completes a `install_snapshot` that a crash interrupted, driven by
    /// the `install.json` record it left behind, and removes that record before
    /// returning. Recovery therefore runs exactly once, and a later crash cannot
    /// replay a discard over entries appended after this call.
    ///
    /// # Errors
    /// `FileStorageError::Io` if the directory or a file cannot be read or the
    /// interrupted install cannot be completed.
    /// `FileStorageError::Corrupt` if a file does not parse.
    pub fn open(dir: &Path) -> Result<Self, FileStorageError> {
        fs::create_dir_all(dir)?;
        let meta = Self::read_meta(dir)?;
        let snapshot = Self::read_snapshot(dir)?;
        let (claimed_first_index, entries) = Self::read_log(dir)?;
        let intent = Self::read_install_intent(dir)?;

        let log = Self::reconcile_log(
            snapshot.as_ref(),
            intent.as_ref(),
            claimed_first_index,
            entries,
        );

        let storage = Self {
            dir: dir.to_path_buf(),
            current_term: meta.current_term,
            voted_for: meta.voted_for,
            snapshot,
            log,
        };

        if intent.is_some() {
            storage.rewrite_log_file()?;
            storage.clear_install_intent()?;
        }

        Ok(storage)
    }

    /// Rebuilds the in-memory log from the files on disk, resolving an
    /// interrupted `install_snapshot`.
    ///
    /// An intent record whose boundary matches the stored snapshot means the
    /// snapshot half reached disk, so the install counts as committed and its
    /// disposition decides the log. An intent that does not match names an
    /// install whose snapshot never landed; the log half had not run either, so
    /// what is on disk still stands.
    ///
    /// Without the record a discarded suffix is indistinguishable from a
    /// retained one, because the entry whose term settled the question is inside
    /// the compacted prefix and no longer readable.
    fn reconcile_log(
        snapshot: Option<&Snapshot>,
        intent: Option<&InstallIntent>,
        claimed_first_index: LogIndex,
        entries: Vec<LogEntry<Cmd>>,
    ) -> Log<Cmd> {
        let Some(snap) = snapshot else {
            return Log::from_entries(entries);
        };

        let discards_suffix = match intent {
            Some(intent) if intent.covers(snap) => {
                match SuffixDisposition::from(intent.disposition) {
                    SuffixDisposition::Discard => true,
                    SuffixDisposition::Retain => false,
                }
            }
            Some(_) | None => false,
        };
        if discards_suffix {
            return Log::from_snapshot_and_suffix(
                snap.meta.last_index,
                snap.meta.last_term,
                Vec::new(),
            );
        }

        // Entries at or below the boundary belong to a log file written before
        // the snapshot landed. Their positions come from the header, so a
        // compacted file is read at its true indexes rather than from 1.
        let surviving = entries
            .into_iter()
            .enumerate()
            .filter(|(i, _)| claimed_first_index.advance_by(*i as u64) > snap.meta.last_index)
            .map(|(_, entry)| entry)
            .collect();
        Log::from_snapshot_and_suffix(snap.meta.last_index, snap.meta.last_term, surviving)
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

    fn install_intent_path(&self) -> PathBuf {
        self.dir.join("install.json")
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

    fn read_install_intent(dir: &Path) -> Result<Option<InstallIntent>, FileStorageError> {
        let path = dir.join("install.json");
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path)?;
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    /// Reads `log.jsonl` and returns the index its first entry occupies along
    /// with the entries themselves.
    ///
    /// The index comes from the header line when one is present, and is 1
    /// otherwise, which covers both an absent file and one that has only ever
    /// been appended to.
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

    /// Replaces `meta.json` atomically: write a temporary file, fsync it, rename
    /// it into place, then fsync the directory.
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
        // The rename itself is a directory modification, and without this fsync
        // it may not survive a crash even though the file contents did.
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    /// Replaces `snapshot.json` atomically, by the same sequence as `flush_meta`.
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

    /// Writes `install.json` atomically, by the same sequence as `flush_meta`.
    fn flush_install_intent(&self, intent: &InstallIntent) -> Result<(), FileStorageError> {
        let tmp = self.dir.join("install.json.tmp");
        let bytes = serde_json::to_vec(intent)?;
        let mut file = File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, self.install_intent_path())?;
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    /// Removes `install.json`. Its absence is what marks the install complete,
    /// so the unlink is followed by a directory fsync like every other rename
    /// here.
    fn clear_install_intent(&self) -> Result<(), FileStorageError> {
        let path = self.install_intent_path();
        if !path.exists() {
            return Ok(());
        }
        fs::remove_file(&path)?;
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    /// Appends one serialized entry to `log.jsonl` and fsyncs it.
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

    /// Brings the in-memory log into the state `disposition` names.
    fn apply_snapshot_to_log(&mut self, snapshot: &Snapshot, disposition: SuffixDisposition) {
        match disposition {
            SuffixDisposition::Retain => self
                .log
                .compact_through(snapshot.meta.last_index, snapshot.meta.last_term),
            SuffixDisposition::Discard => self
                .log
                .reset_to_snapshot(snapshot.meta.last_index, snapshot.meta.last_term),
        }
    }

    /// Rewrites `log.jsonl` from the in-memory log, atomically and with an fsync.
    ///
    /// The output always leads with a header line. Without the first index it
    /// records, a reload could not tell where a surviving file's entries sit
    /// relative to the snapshot boundary, and so could not detect a file left
    /// behind by a crash mid-install.
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

    /// Persists term and vote in a single `flush_meta`, so the pair costs one
    /// fsync rather than two. `Node` calls this only when one of them actually
    /// changed, which leaves a steady-state heartbeat free of fsyncs entirely.
    fn set_meta(&mut self, term: Term, voted_for: Option<NodeId>) -> Result<(), Self::Error> {
        self.current_term = term;
        self.voted_for = voted_for;
        self.flush_meta()
    }

    fn truncate_from(&mut self, index: LogIndex) -> Result<(), Self::Error> {
        self.log.truncate_from(index);
        self.rewrite_log_file()
    }

    /// Appends the suffix `Node` has already reconciled in memory.
    ///
    /// Each entry is written and fsynced on its own. A crash partway through a
    /// batch then leaves a shorter but well-formed log, rather than a truncated
    /// final line that would fail to parse on reload.
    fn append(&mut self, entries: &[LogEntry<Cmd>]) -> Result<(), Self::Error> {
        for entry in entries {
            self.append_to_log_file(entry)?;
            self.log.append(entry.clone());
        }
        Ok(())
    }

    /// Runs the install as one recoverable transaction: record the intent, make
    /// the snapshot durable, bring the log into the state the intent names, then
    /// clear the record.
    ///
    /// The record goes down first because the decision it carries cannot be
    /// recomputed later. Once the snapshot replaces the prefix, the local term
    /// at the boundary is gone, and a retained suffix looks exactly like one
    /// that a conflict had already invalidated. A crash at any point leaves
    /// `open` able to finish or roll back.
    fn install_snapshot(
        &mut self,
        snapshot: &Snapshot,
        disposition: SuffixDisposition,
    ) -> Result<(), Self::Error> {
        self.flush_install_intent(&InstallIntent::new(snapshot, disposition))?;
        self.flush_snapshot(snapshot)?;
        self.snapshot = Some(snapshot.clone());
        self.apply_snapshot_to_log(snapshot, disposition);
        self.rewrite_log_file()?;
        self.clear_install_intent()
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
            // The incoming entry at index 2 carries term 2 against the stored
            // term 1. Node resolves that in memory and issues a truncation
            // followed by the replacement suffix, so storage never detects the
            // conflict itself.
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

        s.install_snapshot(&test_snapshot(2, 1, vec![9, 9]), SuffixDisposition::Retain)
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
            s.install_snapshot(&snapshot, SuffixDisposition::Retain)
                .expect("install snapshot");
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
            // Neither truncate_from nor install_snapshot has run, so log.jsonl
            // still has no header. That is the state a crash between the two
            // durable writes of install_snapshot leaves behind.
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

    /// The durable step of `install_snapshot` after which a crash is simulated.
    #[derive(Clone, Copy)]
    enum CrashAfter {
        IntentRecorded,
        SnapshotDurable,
        LogRewritten,
        IntentCleared,
    }

    /// Runs the durable steps of `install_snapshot` in production order and
    /// stops after `crash_after`, leaving the directory in exactly the state a
    /// crash at that point would.
    fn install_snapshot_interrupted(
        storage: &mut FileStorage<String>,
        snapshot: &Snapshot,
        disposition: SuffixDisposition,
        crash_after: CrashAfter,
    ) {
        storage
            .flush_install_intent(&InstallIntent::new(snapshot, disposition))
            .expect("write install intent");
        if matches!(crash_after, CrashAfter::IntentRecorded) {
            return;
        }
        storage.flush_snapshot(snapshot).expect("write snapshot");
        if matches!(crash_after, CrashAfter::SnapshotDurable) {
            return;
        }
        storage.apply_snapshot_to_log(snapshot, disposition);
        storage.rewrite_log_file().expect("rewrite log");
        if matches!(crash_after, CrashAfter::LogRewritten) {
            return;
        }
        storage
            .clear_install_intent()
            .expect("clear install intent");
    }

    fn reopen_after_interrupted_install(
        disposition: SuffixDisposition,
        boundary_term: u64,
        crash_after: CrashAfter,
    ) -> FileStorage<String> {
        let tmp = tempfile::tempdir().expect("tempdir");
        let snapshot = test_snapshot(2, boundary_term, vec![4, 2]);
        {
            let mut s = open_fresh(tmp.path());
            s.append(&three_entry_log()).expect("append");
            install_snapshot_interrupted(&mut s, &snapshot, disposition, crash_after);
        }
        // Leaked deliberately: the reopened handle must outlive the guard, and
        // the directory is removed with the process.
        let dir = tmp.keep();
        FileStorage::open(&dir).expect("reopen")
    }

    /// A conflicting boundary invalidates the whole log. Once `snapshot.json` is
    /// durable the install has happened, so no later crash may bring the suffix
    /// back.
    #[test]
    fn reopen_discards_suffix_when_conflicting_install_crashes_after_snapshot() {
        for crash_after in [
            CrashAfter::SnapshotDurable,
            CrashAfter::LogRewritten,
            CrashAfter::IntentCleared,
        ] {
            let s = reopen_after_interrupted_install(SuffixDisposition::Discard, 9, crash_after);
            let loaded = s.load().expect("load");

            assert!(
                loaded.entries.is_empty(),
                "a term 9 boundary at index 2 conflicts with the local term 1 entry, \
                 so the entire log goes"
            );
            assert_eq!(
                loaded.snapshot.map(|snap| snap.meta.last_term),
                Some(Term::from(9))
            );
        }
    }

    /// A matching boundary leaves the entries above it valid, so the same crash
    /// points must keep them.
    #[test]
    fn reopen_keeps_suffix_when_matching_install_crashes_after_snapshot() {
        for crash_after in [
            CrashAfter::SnapshotDurable,
            CrashAfter::LogRewritten,
            CrashAfter::IntentCleared,
        ] {
            let s = reopen_after_interrupted_install(SuffixDisposition::Retain, 1, crash_after);
            let loaded = s.load().expect("load");

            assert_eq!(loaded.entries.len(), 1);
            assert_eq!(
                loaded.entries[0].payload,
                LogPayload::Command("SET status=active".into())
            );
        }
    }

    /// The record alone commits nothing. Without the snapshot beside it the
    /// install never happened, so the log must survive untouched.
    #[test]
    fn reopen_rolls_back_install_that_crashed_before_the_snapshot_landed() {
        let s = reopen_after_interrupted_install(
            SuffixDisposition::Discard,
            9,
            CrashAfter::IntentRecorded,
        );
        let loaded = s.load().expect("load");

        assert_eq!(loaded.entries.len(), 3);
        assert!(loaded.snapshot.is_none());
    }

    /// Recovery must consume the record, not merely read it. A record left in
    /// place would replay its discard over entries appended after recovery and
    /// erase them.
    #[test]
    fn reopen_clears_the_install_record_so_later_appends_survive() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let snapshot = test_snapshot(2, 9, vec![4, 2]);
        {
            let mut s = open_fresh(tmp.path());
            s.append(&three_entry_log()).expect("append");
            install_snapshot_interrupted(
                &mut s,
                &snapshot,
                SuffixDisposition::Discard,
                CrashAfter::SnapshotDurable,
            );
        }
        {
            let mut s = open_fresh(tmp.path());
            assert!(!tmp.path().join("install.json").exists());
            s.append(&[LogEntry {
                term: Term::from(9),
                payload: LogPayload::Command("SET region=eu-west-1".into()),
            }])
            .expect("append after recovery");
        }

        let loaded = open_fresh(tmp.path()).load().expect("load");
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(
            loaded.entries[0].payload,
            LogPayload::Command("SET region=eu-west-1".into())
        );
    }

    /// A completed install leaves no record behind, so a plain restart takes the
    /// ordinary path rather than the recovery one.
    #[test]
    fn install_snapshot_removes_its_record_on_success() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut s = open_fresh(tmp.path());
        s.append(&three_entry_log()).expect("append");

        s.install_snapshot(&test_snapshot(2, 9, vec![4, 2]), SuffixDisposition::Discard)
            .expect("install snapshot");

        assert!(!tmp.path().join("install.json").exists());
        assert!(s.load().expect("load").entries.is_empty());
    }

    #[test]
    fn install_snapshot_with_discard_drops_the_whole_log() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut s = open_fresh(tmp.path());
        s.append(&three_entry_log()).expect("append");

        s.install_snapshot(&test_snapshot(2, 9, vec![4, 2]), SuffixDisposition::Discard)
            .expect("install snapshot");

        let loaded = s.load().expect("load");
        assert!(loaded.entries.is_empty());
        assert_eq!(
            loaded.snapshot.map(|snap| snap.meta.last_term),
            Some(Term::from(9))
        );
    }

    #[test]
    fn corrupt_install_record_returns_corrupt_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::write(tmp.path().join("install.json"), b"not valid json")
            .expect("write corrupt install record");

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
