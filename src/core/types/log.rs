use serde::Deserialize;
use serde::Serialize;

use super::config::ClusterConfig;
use super::primitives::LogIndex;
use super::primitives::Term;

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
/// may have removed the entry holding the currently active `ConfigChange`), and
/// to tell durable storage exactly what changed instead of re-deriving it later.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeOutcome {
    /// No conflicting entries; any new entries were appended after the existing suffix.
    Appended,
    /// A conflicting entry (same index, different term) was found at `from`; the log
    /// was truncated there and the new suffix appended in its place.
    Truncated { from: LogIndex },
}

/// The term at a given index, distinguishing "gone via compaction" from "known"
/// from "not appended yet" — callers must pick an explicit response for each.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TermLookup {
    /// An in-log entry, or the snapshot/sentinel boundary itself.
    Known(Term),
    /// Below the snapshot boundary: the term was discarded by compaction.
    Compacted,
    /// Past the last entry in the log.
    BeyondEnd,
}

/// The 1-based replicated log. Owns all index arithmetic so callers never touch
/// array offsets directly. After compaction, `entries[0]` holds
/// `snapshot_last_index + 1`, not index 1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Log<Cmd> {
    /// Index+term of the last compacted entry; both default (zero) pre-compaction,
    /// which degenerates to the original sentinel semantics with no special-casing.
    snapshot_last_index: LogIndex,
    snapshot_last_term: Term,
    entries: Vec<LogEntry<Cmd>>,
}

impl<Cmd> Log<Cmd> {
    pub fn new() -> Self {
        Self {
            snapshot_last_index: LogIndex::default(),
            snapshot_last_term: Term::default(),
            entries: Vec::new(),
        }
    }

    pub fn from_entries(entries: Vec<LogEntry<Cmd>>) -> Self {
        Self {
            snapshot_last_index: LogIndex::default(),
            snapshot_last_term: Term::default(),
            entries,
        }
    }

    /// Reconstructs a log from a persisted snapshot boundary and the surviving
    /// suffix, which the caller guarantees starts at `snapshot_last_index + 1`.
    /// For storage backends restoring state at startup; other callers reach a
    /// compacted state via `compact_through`/`reset_to_snapshot` instead.
    pub fn from_snapshot_and_suffix(
        snapshot_last_index: LogIndex,
        snapshot_last_term: Term,
        entries: Vec<LogEntry<Cmd>>,
    ) -> Self {
        Self {
            snapshot_last_index,
            snapshot_last_term,
            entries,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn last(&self) -> Option<&LogEntry<Cmd>> {
        self.entries.last()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, LogEntry<Cmd>> {
        self.entries.iter()
    }

    /// Array-index lookup for `index`, or `None` if compacted away or out of range.
    fn slot(&self, index: LogIndex) -> Option<usize> {
        if index <= self.snapshot_last_index {
            return None;
        }
        let offset = index.value_since(self.snapshot_last_index)? - 1;
        let offset = usize::try_from(offset).ok()?;
        if offset < self.entries.len() {
            Some(offset)
        } else {
            None
        }
    }

    pub fn snapshot_last_index(&self) -> LogIndex {
        self.snapshot_last_index
    }

    pub fn snapshot_last_term(&self) -> Term {
        self.snapshot_last_term
    }

    /// Index of the oldest entry still in `entries` (or the next index to be
    /// appended, if `entries` is empty).
    pub fn first_index(&self) -> LogIndex {
        self.snapshot_last_index.next()
    }

    pub fn last_index(&self) -> LogIndex {
        self.snapshot_last_index
            .advance_by(self.entries.len() as u64)
    }

    /// Falls back to the snapshot boundary term when `entries` is empty —
    /// required for §5.4.1 vote comparison after full compaction.
    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map_or(self.snapshot_last_term, |entry| entry.term)
    }

    pub fn term_at(&self, index: LogIndex) -> TermLookup {
        if index == self.snapshot_last_index {
            return TermLookup::Known(self.snapshot_last_term);
        }
        if index < self.snapshot_last_index {
            return TermLookup::Compacted;
        }
        match self.slot(index) {
            Some(idx) => TermLookup::Known(self.entries[idx].term),
            None => TermLookup::BeyondEnd,
        }
    }

    pub fn entry(&self, index: LogIndex) -> Option<&LogEntry<Cmd>> {
        self.slot(index).map(|idx| &self.entries[idx])
    }

    /// Entries from `index` (1-based, inclusive) to the end. Any index at or
    /// below the snapshot boundary returns the whole retained log.
    pub fn suffix_from(&self, index: LogIndex) -> &[LogEntry<Cmd>] {
        if index <= self.snapshot_last_index {
            return &self.entries;
        }
        match self.slot(index) {
            Some(idx) => &self.entries[idx..],
            None => &[],
        }
    }

    /// Appends one entry and returns its assigned index.
    pub fn append(&mut self, entry: LogEntry<Cmd>) -> LogIndex {
        self.entries.push(entry);
        self.last_index()
    }

    /// Inclusive: the entry at `index` is also removed. No-op below the
    /// snapshot boundary (already compacted) or past the end of the log.
    pub fn truncate_from(&mut self, index: LogIndex) {
        if let Some(idx) = self.slot(index) {
            self.entries.truncate(idx);
        }
    }

    /// §5.3 Log Matching Property: true if our entry at `prev_index` has term
    /// `prev_term`. Any index below the snapshot boundary is trivially true —
    /// it names a prefix already known to be committed and identical here.
    pub fn matches(&self, prev_index: LogIndex, prev_term: Term) -> bool {
        if prev_index < self.snapshot_last_index {
            return true;
        }
        if prev_index == self.snapshot_last_index {
            return prev_term == self.snapshot_last_term;
        }
        self.slot(prev_index)
            .is_some_and(|idx| self.entries[idx].term == prev_term)
    }

    /// §5.3, Figure 2 AppendEntries §3-5: on conflict (same index, different term),
    /// truncate at the conflict point and replace with the new suffix. An entry whose
    /// index already holds a matching term is treated as already present and skipped.
    /// Entries at or below the snapshot boundary are already covered by the snapshot
    /// and cannot conflict with it, so they are skipped silently.
    pub fn merge(&mut self, prev_index: LogIndex, entries: Vec<LogEntry<Cmd>>) -> MergeOutcome {
        let mut insert_index = prev_index.next();
        let mut outcome = MergeOutcome::Appended;

        for entry in entries {
            if insert_index <= self.snapshot_last_index {
                insert_index = insert_index.next();
                continue;
            }
            match self.slot(insert_index) {
                Some(idx) => {
                    if self.entries[idx].term != entry.term {
                        self.entries.truncate(idx);
                        self.entries.push(entry);
                        outcome = MergeOutcome::Truncated { from: insert_index };
                    }
                    // Same term at this index: entry already present, skip.
                }
                None => self.entries.push(entry),
            }
            insert_index = insert_index.next();
        }

        outcome
    }

    /// Drops entries `[.., index]` and records the compacted boundary. No-op if
    /// `index` is at or below the current boundary.
    pub fn compact_through(&mut self, index: LogIndex, term: Term) {
        if index <= self.snapshot_last_index {
            return;
        }
        if let Some(idx) = self.slot(index) {
            self.entries.drain(..=idx);
        } else {
            // index is beyond the retained suffix (e.g. equals last_index with
            // no gap) — nothing left to keep either way.
            self.entries.clear();
        }
        self.snapshot_last_index = index;
        self.snapshot_last_term = term;
    }

    /// Discards all entries and sets the snapshot boundary directly — used when
    /// installing a snapshot whose boundary conflicts with or extends past the
    /// local log.
    pub fn reset_to_snapshot(&mut self, last_index: LogIndex, last_term: Term) {
        self.entries.clear();
        self.snapshot_last_index = last_index;
        self.snapshot_last_term = last_term;
    }
}

impl<Cmd> Default for Log<Cmd> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::core::types::NodeId;

    fn entry(term: u64, cmd: &str) -> LogEntry<String> {
        LogEntry {
            term: Term::from(term),
            payload: LogPayload::Command(cmd.to_string()),
        }
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
        assert_eq!(
            log.entry(LogIndex::from(2)),
            Some(&entry(1, "SET counter=1"))
        );
    }

    #[test]
    fn merge_truncates_on_same_length_conflict() {
        let mut log = log_with(vec![
            entry(1, "SET name=miles"),
            entry(1, "SET status=pending"),
        ]);

        let outcome = log.merge(LogIndex::from(1), vec![entry(2, "SET status=active")]);

        assert_eq!(
            outcome,
            MergeOutcome::Truncated {
                from: LogIndex::from(2)
            }
        );
        assert_eq!(log.len(), 2);
        assert_eq!(
            log.entry(LogIndex::from(2)),
            Some(&entry(2, "SET status=active"))
        );
    }

    #[test]
    fn merge_truncates_and_grows_the_log() {
        let mut log = log_with(vec![
            entry(1, "SET name=miles"),
            entry(1, "SET status=pending"),
        ]);

        let outcome = log.merge(
            LogIndex::from(1),
            vec![
                entry(2, "SET status=active"),
                entry(2, "SET region=eu-west-1"),
            ],
        );

        assert_eq!(
            outcome,
            MergeOutcome::Truncated {
                from: LogIndex::from(2)
            }
        );
        assert_eq!(log.len(), 3);
        assert_eq!(
            log.entry(LogIndex::from(2)),
            Some(&entry(2, "SET status=active"))
        );
        assert_eq!(
            log.entry(LogIndex::from(3)),
            Some(&entry(2, "SET region=eu-west-1"))
        );
    }

    #[test]
    fn merge_skips_entries_already_present_with_matching_term() {
        let mut log = log_with(vec![entry(1, "SET name=miles"), entry(1, "SET counter=1")]);

        // Retried AppendEntries: same entries, no conflict, nothing to do.
        let outcome = log.merge(
            LogIndex::from(0),
            vec![entry(1, "SET name=miles"), entry(1, "SET counter=1")],
        );

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
        assert_eq!(
            log.term_at(LogIndex::default()),
            TermLookup::Known(Term::default())
        );
    }

    #[test]
    fn term_at_out_of_bounds_is_beyond_end() {
        let log = log_with(vec![entry(1, "SET name=miles")]);
        assert_eq!(log.term_at(LogIndex::from(5)), TermLookup::BeyondEnd);
    }

    #[test]
    fn append_returns_assigned_index() {
        let mut log: Log<String> = Log::new();
        assert_eq!(log.append(entry(1, "SET name=miles")), LogIndex::from(1));
        assert_eq!(log.append(entry(1, "SET counter=1")), LogIndex::from(2));
    }

    #[test]
    fn config_change_payload_round_trips_through_entry() {
        let members =
            std::collections::HashMap::from([(NodeId::from(1), "127.0.0.1:9001".parse().unwrap())]);
        let cfg = ClusterConfig::new(members).unwrap();
        let mut log: Log<String> = Log::new();
        log.append(LogEntry {
            term: Term::from(1),
            payload: LogPayload::ConfigChange(cfg.clone()),
        });

        assert!(
            matches!(&log.entry(LogIndex::from(1)).unwrap().payload, LogPayload::ConfigChange(c) if c == &cfg)
        );
    }

    fn compacted_log(entries: Vec<LogEntry<String>>, through: u64, term: u64) -> Log<String> {
        let mut log = log_with(entries);
        log.compact_through(LogIndex::from(through), Term::from(term));
        log
    }

    #[test]
    fn compact_through_drops_prefix_and_preserves_suffix_indices() {
        let mut log = log_with(vec![
            entry(1, "SET name=miles"),
            entry(1, "SET status=pending"),
            entry(2, "SET status=active"),
        ]);

        log.compact_through(LogIndex::from(2), Term::from(1));

        assert_eq!(log.first_index(), LogIndex::from(3));
        assert_eq!(log.last_index(), LogIndex::from(3));
        assert_eq!(log.snapshot_last_index(), LogIndex::from(2));
        assert_eq!(log.snapshot_last_term(), Term::from(1));
        assert_eq!(
            log.entry(LogIndex::from(3)),
            Some(&entry(2, "SET status=active"))
        );
        assert_eq!(log.entry(LogIndex::from(2)), None);
    }

    #[test]
    fn compact_through_below_boundary_is_noop() {
        let mut log = compacted_log(
            vec![
                entry(1, "SET name=miles"),
                entry(1, "SET status=pending"),
                entry(2, "SET status=active"),
            ],
            2,
            1,
        );

        log.compact_through(LogIndex::from(1), Term::from(99));

        assert_eq!(log.snapshot_last_index(), LogIndex::from(2));
        assert_eq!(log.snapshot_last_term(), Term::from(1));
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn last_term_falls_back_to_snapshot_term_when_log_empty() {
        let log = compacted_log(vec![entry(1, "SET name=miles")], 1, 3);

        assert_eq!(log.last_term(), Term::from(3));
        assert_eq!(log.last_index(), LogIndex::from(1));
    }

    #[test]
    fn term_at_returns_compacted_below_boundary() {
        let log = compacted_log(
            vec![entry(1, "SET name=miles"), entry(2, "SET status=active")],
            1,
            1,
        );

        assert_eq!(log.term_at(LogIndex::default()), TermLookup::Compacted);
    }

    #[test]
    fn term_at_known_at_boundary() {
        let log = compacted_log(
            vec![entry(1, "SET name=miles"), entry(2, "SET status=active")],
            1,
            1,
        );

        assert_eq!(
            log.term_at(LogIndex::from(1)),
            TermLookup::Known(Term::from(1))
        );
    }

    #[test]
    fn term_at_beyond_end_past_last() {
        let log = compacted_log(vec![entry(1, "SET name=miles")], 1, 1);

        assert_eq!(log.term_at(LogIndex::from(9)), TermLookup::BeyondEnd);
    }

    #[test]
    fn matches_true_for_any_index_below_snapshot_boundary() {
        let log = compacted_log(
            vec![
                entry(1, "SET name=miles"),
                entry(1, "SET status=pending"),
                entry(2, "SET status=active"),
            ],
            2,
            1,
        );

        assert!(log.matches(LogIndex::from(1), Term::from(99)));
        assert!(log.matches(LogIndex::default(), Term::from(99)));
    }

    #[test]
    fn matches_compares_snapshot_term_at_boundary() {
        let log = compacted_log(vec![entry(1, "SET name=miles")], 1, 1);

        assert!(log.matches(LogIndex::from(1), Term::from(1)));
        assert!(!log.matches(LogIndex::from(1), Term::from(2)));
    }

    #[test]
    fn merge_skips_entries_covered_by_snapshot() {
        let mut log = compacted_log(
            vec![entry(1, "SET name=miles"), entry(1, "SET status=pending")],
            1,
            1,
        );

        // First entry falls at index 1, at/below the boundary — must be skipped
        // rather than looked up as an array slot (there is none).
        let outcome = log.merge(LogIndex::default(), vec![entry(1, "SET name=miles")]);

        assert_eq!(outcome, MergeOutcome::Appended);
        assert_eq!(log.len(), 1);
        assert_eq!(
            log.entry(LogIndex::from(2)),
            Some(&entry(1, "SET status=pending"))
        );
    }

    #[test]
    fn merge_truncates_on_conflict_after_compaction() {
        let mut log = compacted_log(
            vec![
                entry(1, "SET name=miles"),
                entry(1, "SET status=pending"),
                entry(1, "SET region=eu-west-1"),
            ],
            1,
            1,
        );

        let outcome = log.merge(LogIndex::from(2), vec![entry(2, "SET status=active")]);

        assert_eq!(
            outcome,
            MergeOutcome::Truncated {
                from: LogIndex::from(3)
            }
        );
        assert_eq!(
            log.entry(LogIndex::from(3)),
            Some(&entry(2, "SET status=active"))
        );
    }

    #[test]
    fn reset_to_snapshot_discards_all_entries_and_sets_boundary() {
        let mut log = log_with(vec![entry(1, "SET name=miles"), entry(1, "SET counter=1")]);

        log.reset_to_snapshot(LogIndex::from(5), Term::from(3));

        assert!(log.is_empty());
        assert_eq!(log.snapshot_last_index(), LogIndex::from(5));
        assert_eq!(log.snapshot_last_term(), Term::from(3));
        assert_eq!(log.last_index(), LogIndex::from(5));
        assert_eq!(log.last_term(), Term::from(3));
    }

    #[test]
    fn suffix_from_at_or_below_boundary_returns_whole_log() {
        let log = compacted_log(
            vec![entry(1, "SET name=miles"), entry(2, "SET status=active")],
            1,
            1,
        );

        assert_eq!(log.suffix_from(LogIndex::from(1)).len(), 1);
        assert_eq!(log.suffix_from(LogIndex::default()).len(), 1);
    }

    proptest! {
        #[test]
        fn compaction_never_changes_last_index_or_last_term(
            n in 1usize..12,
            compact_at in 0usize..12,
        ) {
            let entries: Vec<LogEntry<String>> = (0..n)
                .map(|i| entry((i / 3 + 1) as u64, &format!("SET counter={i}")))
                .collect();
            let mut log = log_with(entries.clone());

            let before_last_index = log.last_index();
            let before_last_term = log.last_term();

            let compact_at = compact_at.min(n.saturating_sub(1));
            if n > 0 {
                let boundary_term = entries[compact_at].term;
                log.compact_through(LogIndex::from((compact_at + 1) as u64), boundary_term);

                prop_assert_eq!(log.last_index(), before_last_index);
                prop_assert_eq!(log.last_term(), before_last_term);

                for (i, surviving) in entries.iter().enumerate().skip(compact_at + 1) {
                    let idx = LogIndex::from((i + 1) as u64);
                    prop_assert_eq!(log.term_at(idx), TermLookup::Known(surviving.term));
                }
            }
        }
    }
}
