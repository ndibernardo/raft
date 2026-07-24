use std::fmt;

use serde::Deserialize;
use serde::Serialize;

/// Monotonically increasing term number.
///
/// Terms act as logical clocks in Raft and are used to detect stale information.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Term {
    value: u64,
}

impl Term {
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

/// 1-based log index.
///
/// LogIndex 0 represents "no entries" or "before the first entry".
/// Valid log entries start at index 1.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct LogIndex {
    value: u64,
}

impl LogIndex {
    /// Create from array length (0-based length becomes 1-based index).
    pub fn from_length(len: usize) -> LogIndex {
        LogIndex { value: len as u64 }
    }

    pub fn next(self) -> LogIndex {
        LogIndex {
            value: self.value.saturating_add(1),
        }
    }

    pub fn prev(self) -> Option<LogIndex> {
        if self.value == 0 {
            None
        } else {
            Some(LogIndex {
                value: self.value - 1,
            })
        }
    }

    /// Returns `self` advanced by `n`.
    pub fn advance_by(self, n: u64) -> LogIndex {
        LogIndex {
            value: self.value.saturating_add(n),
        }
    }

    /// Number of steps from `base` to `self`. `None` if `self <= base`.
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

/// Unique server identifier.
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
