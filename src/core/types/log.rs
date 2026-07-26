use serde::Deserialize;
use serde::Serialize;

use super::config::ClusterConfig;
use super::primitives::LogIndex;
use super::primitives::Term;

/// The data carried by a log entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogPayload<Cmd> {
    /// Appended by a new leader at the start of its term (section 8). Committing
    /// this current-term entry commits every preceding entry through the Log
    /// Matching Property, which is what lets a leader serve reads without
    /// committing an entry from an earlier term directly.
    NoOp,
    /// An application command, opaque to the consensus layer.
    Command(Cmd),
    /// Single-server membership change (dissertation section 4.1). Takes effect
    /// as soon as it is appended, not when it commits.
    ConfigChange(ClusterConfig),
}

/// A single entry in the replicated log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry<Cmd> {
    /// Term of the leader that created the entry. Half of the identity used by
    /// the log matching check.
    pub term: Term,
    pub payload: LogPayload<Cmd>,
}

/// What `Log::merge` did to the log.
///
/// Callers need this for two reasons. A truncation may have removed the entry
/// carrying the active `ClusterConfig`, which forces a rescan of the log for the
/// latest surviving `ConfigChange`. It also tells durable storage precisely
/// which range changed, so storage never has to re-derive a diff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeOutcome {
    /// No conflict. New entries, if any, went after the existing suffix.
    Appended,
    /// An entry at index `from` held a different term than the incoming one. The
    /// log was truncated at `from` and the incoming suffix took its place.
    Truncated { from: LogIndex },
}

/// The result of asking for the term at a given index.
///
/// The three cases are distinguished rather than collapsed into an `Option`
/// because callers respond differently to each: a compacted index means the
/// follower must be caught up with a snapshot, while an index beyond the end
/// means the leader must back up `next_index`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TermLookup {
    /// A retained entry, or the snapshot boundary, or the index 0 sentinel.
    Known(Term),
    /// Below the snapshot boundary. Compaction discarded the term.
    Compacted,
    /// Past the last entry in the log.
    BeyondEnd,
}

/// The one-based replicated log.
///
/// All index arithmetic lives here so no caller converts between a `LogIndex`
/// and a `Vec` offset. The distinction matters after compaction: `entries[0]`
/// then holds index `snapshot_last_index + 1` rather than index 1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Log<Cmd> {
    /// Index of the newest entry folded into a snapshot and dropped. Zero before
    /// any compaction, which coincides with the index 0 sentinel and so needs no
    /// special case anywhere in the arithmetic below.
    snapshot_last_index: LogIndex,
    /// Term of the entry at `snapshot_last_index`, retained for log matching and
    /// for the up-to-date comparison after the log is fully compacted.
    snapshot_last_term: Term,
    /// Entries after the boundary, in index order.
    entries: Vec<LogEntry<Cmd>>,
}

impl<Cmd> Log<Cmd> {
    /// An empty, uncompacted log.
    pub fn new() -> Self {
        Self {
            snapshot_last_index: LogIndex::default(),
            snapshot_last_term: Term::default(),
            entries: Vec::new(),
        }
    }

    /// An uncompacted log whose first entry is index 1.
    pub fn from_entries(entries: Vec<LogEntry<Cmd>>) -> Self {
        Self {
            snapshot_last_index: LogIndex::default(),
            snapshot_last_term: Term::default(),
            entries,
        }
    }

    /// Reconstructs a compacted log from a persisted snapshot boundary and the
    /// surviving suffix, which the caller guarantees begins at
    /// `snapshot_last_index + 1`.
    ///
    /// Intended for storage backends restoring state at startup. A running node
    /// reaches a compacted state through `compact_through` or
    /// `reset_to_snapshot`, both of which maintain the invariant themselves.
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

    /// Number of retained entries. Excludes anything dropped by compaction, so
    /// this is not the same as `last_index`.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether any entry is retained. A fully compacted log is empty even though
    /// its `last_index` is non-zero.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The newest retained entry, or `None` when the log is fully compacted.
    pub fn last(&self) -> Option<&LogEntry<Cmd>> {
        self.entries.last()
    }

    /// Iterates the retained entries in index order.
    pub fn iter(&self) -> std::slice::Iter<'_, LogEntry<Cmd>> {
        self.entries.iter()
    }

    /// Translates a `LogIndex` into an offset into `entries`, or `None` when the
    /// index was compacted away or lies past the end.
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

    /// Index of the newest compacted entry. Zero if nothing was compacted.
    pub fn snapshot_last_index(&self) -> LogIndex {
        self.snapshot_last_index
    }

    /// Term of the entry at `snapshot_last_index`.
    pub fn snapshot_last_term(&self) -> Term {
        self.snapshot_last_term
    }

    /// Index of the oldest retained entry, or the next index to be appended when
    /// nothing is retained.
    pub fn first_index(&self) -> LogIndex {
        self.snapshot_last_index.next()
    }

    /// Index of the newest entry, retained or compacted.
    pub fn last_index(&self) -> LogIndex {
        self.snapshot_last_index
            .advance_by(self.entries.len() as u64)
    }

    /// Term of the entry at `last_index`.
    ///
    /// Falls back to the snapshot boundary term when no entry is retained. A
    /// fully compacted log would otherwise report term 0 and lose every
    /// election under the up-to-date comparison of section 5.4.1.
    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map_or(self.snapshot_last_term, |entry| entry.term)
    }

    /// Term recorded at `index`, or the reason it is unavailable.
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

    /// The entry at `index`, or `None` when it was compacted away or lies past
    /// the end.
    pub fn entry(&self, index: LogIndex) -> Option<&LogEntry<Cmd>> {
        self.slot(index).map(|idx| &self.entries[idx])
    }

    /// Retained entries from `index` inclusive to the end. An `index` at or
    /// below the snapshot boundary yields the whole retained log, since nothing
    /// older than the boundary can be served from entries.
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

    /// Removes every entry at or after `index`, inclusive of `index` itself.
    ///
    /// No-op when `index` is at or below the snapshot boundary, where the
    /// entries are already gone, or past the end of the log.
    pub fn truncate_from(&mut self, index: LogIndex) {
        if let Some(idx) = self.slot(index) {
            self.entries.truncate(idx);
        }
    }

    /// The Log Matching consistency check of section 5.3: whether this log holds
    /// term `prev_term` at `prev_index`.
    ///
    /// An index below the snapshot boundary always matches. Compaction only
    /// covers committed entries, so that prefix is already known to agree with
    /// the leader and there is no term left to compare against.
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

    /// Grafts `entries` onto the log starting at `prev_index + 1`, following
    /// rules 3 to 5 of the AppendEntries receiver in Figure 2 (section 5.3).
    ///
    /// An incoming entry whose index already holds the same term is a
    /// retransmission and is skipped. An incoming entry whose index holds a
    /// different term is a conflict: the log is truncated there and the rest of
    /// the incoming suffix replaces it. Positions at or below the snapshot
    /// boundary are skipped, since a committed prefix cannot conflict.
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
                    // Same term at this index means the entry is already
                    // present. Truncating here would discard a correct suffix.
                }
                None => self.entries.push(entry),
            }
            insert_index = insert_index.next();
        }

        outcome
    }

    /// Drops every entry up to and including `index` and records the new
    /// boundary. No-op when `index` is at or below the current boundary.
    ///
    /// The caller must pass the term of the entry at `index`, because that term
    /// outlives the entry itself and is still needed for log matching.
    pub fn compact_through(&mut self, index: LogIndex, term: Term) {
        if index <= self.snapshot_last_index {
            return;
        }
        if let Some(idx) = self.slot(index) {
            self.entries.drain(..=idx);
        } else {
            // The index lies beyond the retained suffix, so every entry is
            // covered by the new boundary.
            self.entries.clear();
        }
        self.snapshot_last_index = index;
        self.snapshot_last_term = term;
    }

    /// Discards every entry and sets the snapshot boundary directly.
    ///
    /// Used when installing a leader's snapshot whose boundary either extends
    /// past the local log or conflicts with it. Unlike `compact_through`, this
    /// makes no assumption that the retained entries agree with the snapshot.
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

        // A retried AppendEntries carrying entries the follower already has.
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

        // The first incoming entry lands on index 1, at the snapshot boundary.
        // It has no array slot, so merge must skip it rather than look it up.
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
