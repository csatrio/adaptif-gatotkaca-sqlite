// ============================================================================
// Import Definition
// ============================================================================

pub use compact_str::CompactString;
pub type GkCompactStr = CompactString;
use crate::{SqliteFastU64Map, crossbeam_utils::Backoff};
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, Null, ToSql, ToSqlOutput, ValueRef};
use smallvec::SmallVec;
use std::{
    cell::RefCell,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

// ============================================================================
// Concrete SQL Parameter Values
// ============================================================================

/// Concrete SQL value with Small-String Optimization (SSO).
/// Strings up to 24 bytes require zero heap allocation.
#[derive(Debug, Clone, PartialEq)]
pub enum GkSqliteValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(GkCompactStr),
    Blob(SmallVec<[u8; 24]>),
}

impl ToSql for GkSqliteValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        // BRANCH: Match and convert value reference
        match self {
            Self::Null => Ok(ToSqlOutput::from(Null)),
            Self::Integer(i) => Ok(ToSqlOutput::from(*i)),
            Self::Real(f) => Ok(ToSqlOutput::from(*f)),
            Self::Text(s) => Ok(ToSqlOutput::from(s.as_str())),
            Self::Blob(b) => Ok(ToSqlOutput::from(b.as_slice())),
        }
    }
}

impl FromSql for GkSqliteValue {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        // BRANCH: Match and parse column value
        match value {
            ValueRef::Null => Ok(Self::Null),
            ValueRef::Integer(i) => Ok(Self::Integer(i)),
            ValueRef::Real(f) => Ok(Self::Real(f)),
            ValueRef::Text(s) => {
                let s_str = std::str::from_utf8(s).map_err(|e| FromSqlError::Other(Box::new(e)))?;
                Ok(Self::Text(GkCompactStr::new(s_str)))
            }
            ValueRef::Blob(b) => Ok(Self::Blob(SmallVec::from_slice(b))),
        }
    }
}

impl From<i64> for GkSqliteValue {
    fn from(v: i64) -> Self {
        Self::Integer(v)
    }
}

impl From<i32> for GkSqliteValue {
    fn from(v: i32) -> Self {
        Self::Integer(v as i64)
    }
}

impl From<usize> for GkSqliteValue {
    fn from(v: usize) -> Self {
        Self::Integer(v as i64)
    }
}

impl From<f64> for GkSqliteValue {
    fn from(v: f64) -> Self {
        Self::Real(v)
    }
}

impl From<&str> for GkSqliteValue {
    fn from(v: &str) -> Self {
        Self::Text(GkCompactStr::new(v))
    }
}

impl From<String> for GkSqliteValue {
    fn from(v: String) -> Self {
        Self::Text(GkCompactStr::new(v))
    }
}

impl From<GkCompactStr> for GkSqliteValue {
    fn from(v: GkCompactStr) -> Self {
        Self::Text(v)
    }
}

impl From<Vec<u8>> for GkSqliteValue {
    fn from(v: Vec<u8>) -> Self {
        Self::Blob(SmallVec::from_vec(v))
    }
}

impl From<SmallVec<[u8; 24]>> for GkSqliteValue {
    fn from(v: SmallVec<[u8; 24]>) -> Self {
        Self::Blob(v)
    }
}

impl<T: Into<GkSqliteValue>> From<Option<T>> for GkSqliteValue {
    fn from(v: Option<T>) -> Self {
        // BRANCH: Convert optional value
        match v {
            Some(val) => val.into(),
            None => Self::Null,
        }
    }
}

/// Zero-allocation parameter storage for <= 4 parameters (covers ~90% of OLTP queries, inline < 140 bytes),
/// with standard library drop/move semantics via SmallVec and transparent spill for larger queries.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct GkSqliteParams(pub SmallVec<[GkSqliteValue; 4]>);

impl std::ops::Deref for GkSqliteParams {
    type Target = [GkSqliteValue];
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for GkSqliteParams {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl GkSqliteParams {
    #[inline(always)]
    pub fn new() -> Self {
        Self(SmallVec::new())
    }

    #[inline(always)]
    pub fn push(&mut self, val: impl Into<GkSqliteValue>) {
        self.0.push(val.into());
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[GkSqliteValue] {
        self.0.as_slice()
    }
}

impl From<SmallVec<[GkSqliteValue; 4]>> for GkSqliteParams {
    #[inline(always)]
    fn from(v: SmallVec<[GkSqliteValue; 4]>) -> Self {
        Self(v)
    }
}

impl From<Vec<GkSqliteValue>> for GkSqliteParams {
    #[inline(always)]
    fn from(v: Vec<GkSqliteValue>) -> Self {
        Self(SmallVec::from_vec(v))
    }
}

impl From<()> for GkSqliteParams {
    #[inline(always)]
    fn from(_: ()) -> Self {
        Self(SmallVec::new())
    }
}

impl<T: Into<GkSqliteValue>> From<(T,)> for GkSqliteParams {
    #[inline(always)]
    fn from((a,): (T,)) -> Self {
        let mut v = SmallVec::new();
        v.push(a.into());
        Self(v)
    }
}

impl<T1: Into<GkSqliteValue>, T2: Into<GkSqliteValue>> From<(T1, T2)> for GkSqliteParams {
    #[inline(always)]
    fn from((a, b): (T1, T2)) -> Self {
        let mut v = SmallVec::new();
        v.push(a.into());
        v.push(b.into());
        Self(v)
    }
}

impl<T1: Into<GkSqliteValue>, T2: Into<GkSqliteValue>, T3: Into<GkSqliteValue>> From<(T1, T2, T3)>
    for GkSqliteParams
{
    #[inline(always)]
    fn from((a, b, c): (T1, T2, T3)) -> Self {
        let mut v = SmallVec::new();
        v.push(a.into());
        v.push(b.into());
        v.push(c.into());
        Self(v)
    }
}

impl<
    T1: Into<GkSqliteValue>,
    T2: Into<GkSqliteValue>,
    T3: Into<GkSqliteValue>,
    T4: Into<GkSqliteValue>,
> From<(T1, T2, T3, T4)> for GkSqliteParams
{
    #[inline(always)]
    fn from((a, b, c, d): (T1, T2, T3, T4)) -> Self {
        let mut v = SmallVec::new();
        v.push(a.into());
        v.push(b.into());
        v.push(c.into());
        v.push(d.into());
        Self(v)
    }
}

impl<T: Into<GkSqliteValue> + Clone, const N: usize> From<[T; N]> for GkSqliteParams {
    fn from(arr: [T; N]) -> Self {
        let mut v = SmallVec::new();
        for item in arr {
            v.push(item.into());
        }
        Self(v)
    }
}

impl<T: Into<GkSqliteValue> + Clone> From<&[T]> for GkSqliteParams {
    fn from(slice: &[T]) -> Self {
        let mut v = SmallVec::new();
        for item in slice {
            v.push(item.clone().into());
        }
        Self(v)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GkSqliteWriteOutcome {
    /// Number of rows modified by the write statement
    pub rows: usize,
    /// Strictly increasing sequence token from `committed` atomic counter
    pub seq: u64,
    /// Last inserted row ID on the writer connection
    pub last_insert_rowid: i64,
}

// ============================================================================
// Atomic Commit Counter
// ============================================================================

/// Atomic counter tracking the highest sequence number whose COMMIT has returned.
#[derive(Debug, Clone)]
pub struct GkSqliteCommitCounter {
    inner: Arc<AtomicU64>,
}

impl GkSqliteCommitCounter {
    pub fn new(initial: u64) -> Self {
        Self {
            inner: Arc::new(AtomicU64::new(initial)),
        }
    }

    #[inline(always)]
    pub fn load(&self, ordering: Ordering) -> u64 {
        self.inner.load(ordering)
    }

    #[inline(always)]
    pub fn store(&self, val: u64, ordering: Ordering) {
        self.inner.store(val, ordering);
    }

    /// Spin and snooze using crossbeam_utils::Backoff until counter >= min_seq or deadline expires.
    pub fn wait_for(&self, min_seq: u64, timeout: Duration) -> bool {
        // BRANCH: Skip wait if min_seq is already satisfied
        if self.load(Ordering::Acquire) >= min_seq {
            return true;
        }

        let backoff = Backoff::new();
        let deadline = Instant::now() + timeout;

        while self.load(Ordering::Acquire) < min_seq {
            // BRANCH: Check deadline expiration
            if Instant::now() >= deadline {
                return false;
            }
            if backoff.is_completed() {
                std::thread::yield_now();
            } else {
                backoff.snooze();
            }
        }
        true
    }
}

// ============================================================================
// Thread-Local Last Write Token
// ============================================================================

thread_local! {
    static LAST_TOKENS: RefCell<SqliteFastU64Map<u64>> = RefCell::new(SqliteFastU64Map::new());
}

pub fn get_last_token(db_id: u64) -> u64 {
    LAST_TOKENS.with(|cell| cell.borrow().get(&db_id).copied().unwrap_or(0))
}

pub fn set_last_token(db_id: u64, token: u64) {
    LAST_TOKENS.with(|cell| {
        cell.borrow_mut().insert(db_id, token);
    });
}
