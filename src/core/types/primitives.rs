use std::fmt;

use serde::Deserialize;
use serde::Serialize;

/// Monotonically increasing term number.
///
/// Terms are the logical clock of the cluster. Every message carries one, and a
/// node that sees a term higher than its own steps down and adopts it, which is
/// how stale leaders and stale votes are detected (section 5.1).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Term {
    value: u64,
}

impl Term {
    /// Returns the following term. Saturates at `u64::MAX` rather than wrapping,
    /// since a wrapped term would make an ancient leader look current.
    pub fn next(self) -> Term {
        Term {
            value: self.value.saturating_add(1),
        }
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "T{}", self.value)
    }
}

impl From<u64> for Term {
    fn from(value: u64) -> Self {
        Term { value }
    }
}

/// One-based position of an entry in the replicated log.
///
/// Index 0 is the sentinel for "no entries" or "before the first entry", so it
/// is a valid value for `commit_index` and `prev_log_index` but never addresses
/// a real entry. The first entry a leader appends has index 1.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct LogIndex {
    value: u64,
}

impl LogIndex {
    /// Converts a slice length into the index of its last element, since a log
    /// of `len` entries starting at 1 ends at index `len`.
    pub fn from_length(len: usize) -> LogIndex {
        LogIndex { value: len as u64 }
    }

    /// Returns the following index. Saturates at `u64::MAX`.
    pub fn next(self) -> LogIndex {
        LogIndex {
            value: self.value.saturating_add(1),
        }
    }

    /// Returns the preceding index, or `None` at the index 0 sentinel.
    pub fn prev(self) -> Option<LogIndex> {
        if self.value == 0 {
            None
        } else {
            Some(LogIndex {
                value: self.value - 1,
            })
        }
    }

    /// Returns `self` advanced by `n` positions. Saturates at `u64::MAX`.
    pub fn advance_by(self, n: u64) -> LogIndex {
        LogIndex {
            value: self.value.saturating_add(n),
        }
    }

    /// Number of positions from `base` up to `self`, or `None` when `self` does
    /// not lie strictly after `base`.
    pub fn value_since(self, base: LogIndex) -> Option<u64> {
        self.value.checked_sub(base.value).filter(|&d| d > 0)
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "I{}", self.value)
    }
}

impl From<u64> for LogIndex {
    fn from(value: u64) -> Self {
        LogIndex { value }
    }
}

/// Cluster-wide unique server identifier. Stable across restarts, because a
/// node's persisted vote and a leader's replication state are both keyed by it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId {
    value: u64,
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "N{}", self.value)
    }
}

impl NodeId {
    /// The underlying numeric identifier.
    pub fn value(self) -> u64 {
        self.value
    }
}

impl From<u64> for NodeId {
    fn from(value: u64) -> Self {
        NodeId { value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_next_increases_by_one() {
        assert_eq!(Term::from(3).next(), Term::from(4));
    }

    #[test]
    fn term_next_saturates_at_max() {
        assert_eq!(Term::from(u64::MAX).next(), Term::from(u64::MAX));
    }

    #[test]
    fn log_index_next_increases_by_one() {
        assert_eq!(LogIndex::from(5).next(), LogIndex::from(6));
    }

    #[test]
    fn log_index_next_saturates_at_max() {
        assert_eq!(LogIndex::from(u64::MAX).next(), LogIndex::from(u64::MAX));
    }

    #[test]
    fn log_index_prev_returns_predecessor() {
        assert_eq!(LogIndex::from(3).prev(), Some(LogIndex::from(2)));
    }

    #[test]
    fn log_index_prev_returns_none_at_zero() {
        assert_eq!(LogIndex::default().prev(), None);
    }

    #[test]
    fn log_index_advance_by_adds_steps() {
        assert_eq!(LogIndex::from(2).advance_by(3), LogIndex::from(5));
    }

    #[test]
    fn log_index_value_since_returns_none_when_not_strictly_greater() {
        assert_eq!(LogIndex::from(2).value_since(LogIndex::from(2)), None);
        assert_eq!(LogIndex::from(1).value_since(LogIndex::from(2)), None);
    }

    #[test]
    fn log_index_value_since_returns_step_count() {
        assert_eq!(LogIndex::from(5).value_since(LogIndex::from(2)), Some(3));
    }

    #[test]
    fn log_index_from_length_maps_array_length_to_last_index() {
        assert_eq!(LogIndex::from_length(0), LogIndex::default());
        assert_eq!(LogIndex::from_length(3), LogIndex::from(3));
    }
}
