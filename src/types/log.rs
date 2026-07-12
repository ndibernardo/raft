use std::ops::{Index, IndexMut};

use serde::{Deserialize, Serialize};

use super::config::ClusterConfig;
use super::primitives::{LogIndex, Term};

/// The data carried by a log entry. §8: leaders append a NoOp on election to commit
/// prior-term entries via Log Matching without direct commitment of old terms.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogPayload<Cmd> {
    NoOp,
    Command(Cmd),
    /// Single-server membership change (dissertation §4.1).
    /// Takes effect immediately when appended, not when committed.
    ConfigChange(ClusterConfig),
}

/// A single entry in the replicated log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry<Cmd> {
    pub term: Term,
    pub payload: LogPayload<Cmd>,
}

/// Whether `Log::merge` found a conflicting entry and truncated the log.
/// Callers use this to decide whether a config rescan is needed (a truncation
/// may have removed the entry holding the currently active `ConfigChange`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeOutcome {
    /// No conflicting entries; any new entries were appended after the existing suffix.
    Appended,
    /// A conflicting entry (same index, different term) was found; the log was
    /// truncated at that point and the new suffix appended in its place.
    Truncated,
}

/// The 1-based replicated log. Owns all index arithmetic so callers never touch
/// array offsets directly (§2.4 audit finding: `to_array_index` bridging was
/// scattered across `node.rs`, `storage.rs`, and `file_storage.rs`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Log<Cmd>(Vec<LogEntry<Cmd>>);

impl<Cmd> Log<Cmd> {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn from_entries(entries: Vec<LogEntry<Cmd>>) -> Self {
        Self(entries)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn last(&self) -> Option<&LogEntry<Cmd>> {
        self.0.last()
    }

    /// Array-index lookup (0-based), for callers already iterating array positions
    /// (e.g. cross-node log comparison in the proptest cluster harness).
    pub fn get(&self, idx: usize) -> Option<&LogEntry<Cmd>> {
        self.0.get(idx)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, LogEntry<Cmd>> {
        self.0.iter()
    }

    pub fn last_index(&self) -> LogIndex {
        LogIndex::from_length(self.0.len())
    }

    pub fn last_term(&self) -> Term {
        self.0.last().map_or(Term::default(), |entry| entry.term)
    }

    /// Index 0 is the implicit sentinel and always returns `Some(Term::default())`;
    /// an out-of-bounds index returns `None`.
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        match index.to_array_index() {
            None => Some(Term::default()),
            Some(idx) => self.0.get(idx).map(|e| e.term),
        }
    }

    pub fn entry(&self, index: LogIndex) -> Option<&LogEntry<Cmd>> {
        index.to_array_index().and_then(|idx| self.0.get(idx))
    }

    /// Entries from `index` (1-based, inclusive) to the end. Index 0 returns the whole log.
    pub fn suffix_from(&self, index: LogIndex) -> &[LogEntry<Cmd>] {
        match index.to_array_index() {
            None => &self.0,
            Some(idx) => self.0.get(idx..).unwrap_or_default(),
        }
    }

    /// Appends one entry and returns its assigned index.
    pub fn append(&mut self, entry: LogEntry<Cmd>) -> LogIndex {
        self.0.push(entry);
        self.last_index()
    }

    /// Inclusive: the entry at `index` is also removed.
    pub fn truncate_from(&mut self, index: LogIndex) {
        if let Some(idx) = index.to_array_index() {
            self.0.truncate(idx);
        }
    }

    /// §5.3 Log Matching Property: true if our entry at `prev_index` has term
    /// `prev_term`. Index 0 is the implicit sentinel, always matching term 0.
    pub fn matches(&self, prev_index: LogIndex, prev_term: Term) -> bool {
        match prev_index.to_array_index() {
            None => prev_term == Term::default(),
            Some(idx) => self.0.get(idx).is_some_and(|entry| entry.term == prev_term),
        }
    }

    /// §5.3, Figure 2 AppendEntries §3-5: on conflict (same index, different term),
    /// truncate at the conflict point and replace with the new suffix. An entry whose
    /// index already holds a matching term is treated as already present and skipped.
    pub fn merge(&mut self, prev_index: LogIndex, entries: Vec<LogEntry<Cmd>>) -> MergeOutcome {
        let mut insert_index = prev_index.next();
        let mut outcome = MergeOutcome::Appended;

        for entry in entries {
            match insert_index.to_array_index() {
                Some(idx) if idx < self.0.len() => {
                    if self.0[idx].term != entry.term {
                        self.0.truncate(idx);
                        self.0.push(entry);
                        outcome = MergeOutcome::Truncated;
                    }
                    // Same term at this index: entry already present, skip.
                }
                _ => self.0.push(entry),
            }
            insert_index = insert_index.next();
        }

        outcome
    }
}

impl<Cmd> Default for Log<Cmd> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Cmd> Index<usize> for Log<Cmd> {
    type Output = LogEntry<Cmd>;

    fn index(&self, idx: usize) -> &LogEntry<Cmd> {
        &self.0[idx]
    }
}

impl<Cmd> IndexMut<usize> for Log<Cmd> {
    fn index_mut(&mut self, idx: usize) -> &mut LogEntry<Cmd> {
        &mut self.0[idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeId;

    fn entry(term: u64, cmd: &str) -> LogEntry<String> {
        LogEntry { term: Term::from(term), payload: LogPayload::Command(cmd.to_string()) }
    }

    fn log_with(entries: Vec<LogEntry<String>>) -> Log<String> {
        Log::from_entries(entries)
    }

    #[test]
    fn merge_appends_when_no_conflict() {
        let mut log = log_with(vec![entry(1, "SET name=miles")]);

        let outcome = log.merge(LogIndex::from(1), vec![entry(1, "SET counter=1")]);

        assert_eq!(outcome, MergeOutcome::Appended);
        assert_eq!(log.len(), 2);
        assert_eq!(log[1], entry(1, "SET counter=1"));
    }

    #[test]
    fn merge_truncates_on_same_length_conflict() {
        let mut log = log_with(vec![entry(1, "SET name=miles"), entry(1, "SET status=pending")]);

        let outcome = log.merge(LogIndex::from(1), vec![entry(2, "SET status=active")]);

        assert_eq!(outcome, MergeOutcome::Truncated);
        assert_eq!(log.len(), 2);
        assert_eq!(log[1], entry(2, "SET status=active"));
    }

    #[test]
    fn merge_truncates_and_grows_the_log() {
        let mut log = log_with(vec![entry(1, "SET name=miles"), entry(1, "SET status=pending")]);

        let outcome = log.merge(
            LogIndex::from(1),
            vec![entry(2, "SET status=active"), entry(2, "SET region=eu-west-1")],
        );

        assert_eq!(outcome, MergeOutcome::Truncated);
        assert_eq!(log.len(), 3);
        assert_eq!(log[1], entry(2, "SET status=active"));
        assert_eq!(log[2], entry(2, "SET region=eu-west-1"));
    }

    #[test]
    fn merge_skips_entries_already_present_with_matching_term() {
        let mut log = log_with(vec![entry(1, "SET name=miles"), entry(1, "SET counter=1")]);

        // Retried AppendEntries: same entries, no conflict, nothing to do.
        let outcome = log.merge(LogIndex::from(0), vec![entry(1, "SET name=miles"), entry(1, "SET counter=1")]);

        assert_eq!(outcome, MergeOutcome::Appended);
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn matches_is_true_for_sentinel_index_zero() {
        let log: Log<String> = Log::new();
        assert!(log.matches(LogIndex::default(), Term::default()));
    }

    #[test]
    fn matches_is_false_when_term_mismatches_at_prev_index() {
        let log = log_with(vec![entry(1, "SET name=miles")]);
        assert!(!log.matches(LogIndex::from(1), Term::from(2)));
    }

    #[test]
    fn suffix_from_zero_returns_whole_log() {
        let log = log_with(vec![entry(1, "SET name=miles"), entry(1, "SET counter=1")]);
        assert_eq!(log.suffix_from(LogIndex::default()).len(), 2);
    }

    #[test]
    fn suffix_from_past_end_returns_empty() {
        let log = log_with(vec![entry(1, "SET name=miles")]);
        assert!(log.suffix_from(LogIndex::from(5)).is_empty());
    }

    #[test]
    fn term_at_sentinel_index_is_default_term() {
        let log: Log<String> = Log::new();
        assert_eq!(log.term_at(LogIndex::default()), Some(Term::default()));
    }

    #[test]
    fn term_at_out_of_bounds_is_none() {
        let log = log_with(vec![entry(1, "SET name=miles")]);
        assert_eq!(log.term_at(LogIndex::from(5)), None);
    }

    #[test]
    fn append_returns_assigned_index() {
        let mut log: Log<String> = Log::new();
        assert_eq!(log.append(entry(1, "SET name=miles")), LogIndex::from(1));
        assert_eq!(log.append(entry(1, "SET counter=1")), LogIndex::from(2));
    }

    #[test]
    fn config_change_payload_round_trips_through_entry() {
        let members = std::collections::HashMap::from([(
            NodeId::from(1),
            "127.0.0.1:9001".parse().unwrap(),
        )]);
        let cfg = ClusterConfig::new(members).unwrap();
        let mut log: Log<String> = Log::new();
        log.append(LogEntry { term: Term::from(1), payload: LogPayload::ConfigChange(cfg.clone()) });

        assert!(matches!(&log[0].payload, LogPayload::ConfigChange(c) if c == &cfg));
    }
}
