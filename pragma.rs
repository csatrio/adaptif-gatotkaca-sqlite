// ============================================================================
// Import Definition
// ============================================================================

use crate::counter::GkCompactStr;
use crate::error::GkSqliteErrorEnum;
use rusqlite::Connection;
use std::fmt;

// ============================================================================
// Error Definition
// ============================================================================

/// Why a pragma configuration was rejected. A `#[repr(transparent)]` code, no heap.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GkPragmaError(u8);

impl GkPragmaError {
    /// A pragma name in `extra` is empty, too long or not `[A-Za-z_][A-Za-z0-9_]*`.
    pub const INVALID_NAME: Self = Self(1);
    /// A value is out of range, or an `extra` text value is empty, too long or has characters outside `[A-Za-z0-9_.-]`.
    pub const INVALID_VALUE: Self = Self(2);
    /// An `extra` entry names a pragma that has a typed field (set it through the field instead).
    pub const FORBIDDEN: Self = Self(3);

    /// The raw error code.
    pub const fn code(self) -> u8 {
        self.0
    }

    /// A static, human readable description of the error.
    pub const fn message(self) -> &'static str {
        // BRANCH: map the code to its message
        match self.0 {
            1 => "invalid pragma name",
            2 => "invalid pragma value",
            3 => "pragma is configured through a typed field and cannot be an extra",
            _ => "unknown pragma error",
        }
    }
}

impl fmt::Display for GkPragmaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for GkPragmaError {}

// ============================================================================
// Typed Values
// ============================================================================

/// `PRAGMA journal_mode`. Default: `Wal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GkJournalMode {
    /// Write-ahead log (readers and the writer do not block each other).
    #[default]
    Wal,
    /// Rollback journal, deleted after each transaction.
    Delete,
    /// Rollback journal, truncated to zero length after each transaction.
    Truncate,
    /// Rollback journal, header zeroed after each transaction.
    Persist,
    /// Rollback journal kept in memory.
    Memory,
    /// No rollback journal (no atomic rollback).
    Off,
}

impl GkJournalMode {
    /// The SQL keyword for this mode.
    pub const fn as_str(self) -> &'static str {
        // BRANCH: one keyword per mode
        match self {
            Self::Wal => "WAL",
            Self::Delete => "DELETE",
            Self::Truncate => "TRUNCATE",
            Self::Persist => "PERSIST",
            Self::Memory => "MEMORY",
            Self::Off => "OFF",
        }
    }
}

/// `PRAGMA synchronous`. Default: `Full` (SQLite's own default); the writer preset uses `Normal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GkSynchronous {
    /// No fsync (fastest, can corrupt on power loss).
    Off,
    /// Fewer fsyncs; safe against corruption in WAL mode.
    Normal,
    /// fsync at every commit.
    #[default]
    Full,
    /// `Full` plus an fsync of the directory.
    Extra,
}

impl GkSynchronous {
    /// The SQL keyword for this level.
    pub const fn as_str(self) -> &'static str {
        // BRANCH: one keyword per level
        match self {
            Self::Off => "OFF",
            Self::Normal => "NORMAL",
            Self::Full => "FULL",
            Self::Extra => "EXTRA",
        }
    }
}

/// `PRAGMA temp_store`. Default: `Default` (compile-time choice of SQLite).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GkTempStore {
    /// Use the compile-time default.
    #[default]
    Default,
    /// Temporary tables and indices in files.
    File,
    /// Temporary tables and indices in memory.
    Memory,
}

impl GkTempStore {
    /// The SQL keyword for this store.
    pub const fn as_str(self) -> &'static str {
        // BRANCH: one keyword per store
        match self {
            Self::Default => "DEFAULT",
            Self::File => "FILE",
            Self::Memory => "MEMORY",
        }
    }
}

/// `PRAGMA locking_mode`. Default: `Normal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GkLockingMode {
    /// Release locks at the end of each transaction.
    #[default]
    Normal,
    /// Keep locks once taken (other connections are locked out).
    Exclusive,
}

impl GkLockingMode {
    /// The SQL keyword for this mode.
    pub const fn as_str(self) -> &'static str {
        // BRANCH: one keyword per mode
        match self {
            Self::Normal => "NORMAL",
            Self::Exclusive => "EXCLUSIVE",
        }
    }
}

/// Value of an `extra` pragma. Values cannot be bound in `PRAGMA`, so `Text` is restricted by `validate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GkPragmaValue {
    /// An integer, emitted as is.
    Int(i64),
    /// A boolean, emitted as `1` / `0`.
    Bool(bool),
    /// Text of at most 64 characters from `[A-Za-z0-9_.-]`.
    Text(GkCompactStr),
}

// ============================================================================
// Pragma Set
// ============================================================================

/// Maximum length of an extra pragma name or text value.
const MAX_TOKEN_LEN: usize = 64;

/// Pragma names owned by typed fields; an extra with one of these names is `FORBIDDEN`.
const TYPED_NAMES: [&str; 11] = [
    "journal_mode",
    "synchronous",
    "cache_size",
    "cache_size_kib",
    "mmap_size",
    "temp_store",
    "wal_autocheckpoint",
    "journal_size_limit",
    "foreign_keys",
    "locking_mode",
    "query_only",
];

/// A value handed to one `PRAGMA name = value` step.
enum GkPragmaArg<'a> {
    Int(i64),
    Text(&'a str),
}

/// A typed, validated set of connection pragmas. Every field is optional: `None` leaves SQLite's default untouched.
///
/// Default (`Default::default()`): same as [`GkSqlitePragmas::none`], nothing is set.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GkSqlitePragmas {
    /// `journal_mode`.
    pub journal_mode: Option<GkJournalMode>,
    /// `synchronous`.
    pub synchronous: Option<GkSynchronous>,
    /// Page cache size in KiB, emitted as a negative `cache_size`. Valid: `1..=i32::MAX` (0 is rejected: it would mean 0 pages, not 0 KiB).
    pub cache_size_kib: Option<i64>,
    /// Memory-map size in bytes (`mmap_size`). Valid: `>= 0`, 0 disables mmap.
    pub mmap_size: Option<i64>,
    /// `temp_store`.
    pub temp_store: Option<GkTempStore>,
    /// `wal_autocheckpoint` in pages. Valid: `0..=i32::MAX`, 0 disables the automatic checkpoint.
    pub wal_autocheckpoint: Option<i64>,
    /// `journal_size_limit` in bytes. Valid: `>= -1`, -1 means no limit.
    pub journal_size_limit: Option<i64>,
    /// `foreign_keys` enforcement.
    pub foreign_keys: Option<bool>,
    /// `locking_mode`.
    pub locking_mode: Option<GkLockingMode>,
    /// `query_only`: refuse writes on this connection.
    pub query_only: Option<bool>,
    /// Escape hatch for pragmas without a typed field; applied last, in order.
    pub extra: Vec<(GkCompactStr, GkPragmaValue)>,
}

impl GkSqlitePragmas {
    /// Nothing set: `apply` does not touch the connection.
    pub const fn none() -> Self {
        Self {
            journal_mode: None,
            synchronous: None,
            cache_size_kib: None,
            mmap_size: None,
            temp_store: None,
            wal_autocheckpoint: None,
            journal_size_limit: None,
            foreign_keys: None,
            locking_mode: None,
            query_only: None,
            extra: Vec::new(),
        }
    }

    /// The writer connection as shipped: `journal_mode=WAL`, `synchronous=NORMAL`.
    pub const fn writer_default() -> Self {
        Self::none()
            .with_journal_mode(GkJournalMode::Wal)
            .with_synchronous(GkSynchronous::Normal)
    }

    /// The reader connection as shipped: `query_only=ON`, `mmap_size=268435456`, `cache_size=-64000` (64000 KiB).
    pub const fn reader_default() -> Self {
        Self::none()
            .with_query_only(true)
            .with_mmap_size(268_435_456)
            .with_cache_size_kib(64_000)
    }

    /// Writer preset for bulk throughput: WAL, `NORMAL`, `wal_autocheckpoint=4000`, memory temp store, 16384 KiB cache.
    pub const fn writer_throughput() -> Self {
        Self::writer_default()
            .with_wal_autocheckpoint(4000)
            .with_temp_store(GkTempStore::Memory)
            .with_cache_size_kib(16_384)
    }

    /// Writer preset for durability: WAL with `synchronous=FULL`.
    pub const fn writer_durable() -> Self {
        Self::writer_default().with_synchronous(GkSynchronous::Full)
    }

    /// Reader preset for small footprint: `query_only`, 2048 KiB cache, mmap disabled.
    pub const fn reader_low_memory() -> Self {
        Self::none()
            .with_query_only(true)
            .with_cache_size_kib(2048)
            .with_mmap_size(0)
    }

    /// Sets `journal_mode`.
    pub const fn with_journal_mode(mut self, v: GkJournalMode) -> Self {
        self.journal_mode = Some(v);
        self
    }

    /// Sets `synchronous`.
    pub const fn with_synchronous(mut self, v: GkSynchronous) -> Self {
        self.synchronous = Some(v);
        self
    }

    /// Sets the page cache size in KiB.
    pub const fn with_cache_size_kib(mut self, v: i64) -> Self {
        self.cache_size_kib = Some(v);
        self
    }

    /// Sets `mmap_size` in bytes.
    pub const fn with_mmap_size(mut self, v: i64) -> Self {
        self.mmap_size = Some(v);
        self
    }

    /// Sets `temp_store`.
    pub const fn with_temp_store(mut self, v: GkTempStore) -> Self {
        self.temp_store = Some(v);
        self
    }

    /// Sets `wal_autocheckpoint` in pages.
    pub const fn with_wal_autocheckpoint(mut self, v: i64) -> Self {
        self.wal_autocheckpoint = Some(v);
        self
    }

    /// Sets `journal_size_limit` in bytes.
    pub const fn with_journal_size_limit(mut self, v: i64) -> Self {
        self.journal_size_limit = Some(v);
        self
    }

    /// Sets `foreign_keys`.
    pub const fn with_foreign_keys(mut self, v: bool) -> Self {
        self.foreign_keys = Some(v);
        self
    }

    /// Sets `locking_mode`.
    pub const fn with_locking_mode(mut self, v: GkLockingMode) -> Self {
        self.locking_mode = Some(v);
        self
    }

    /// Sets `query_only`.
    pub const fn with_query_only(mut self, v: bool) -> Self {
        self.query_only = Some(v);
        self
    }

    /// Appends an extra pragma (validated by `validate` / `apply`).
    pub fn with_extra(mut self, name: impl Into<GkCompactStr>, value: GkPragmaValue) -> Self {
        self.extra.push((name.into(), value));
        self
    }

    /// Checks every value. This is what keeps `PRAGMA` text injection-free, since pragma values cannot be bound:
    /// extra names must match `[A-Za-z_][A-Za-z0-9_]*` (max 64), extra text `[A-Za-z0-9_.-]+` (max 64), extras
    /// must not repeat a typed pragma name (`FORBIDDEN`), typed numbers must be in their documented range.
    pub fn validate(&self) -> Result<(), GkPragmaError> {
        // BRANCH: cache_size_kib range
        if let Some(v) = self.cache_size_kib
            && !(1..=i64::from(i32::MAX)).contains(&v)
        {
            return Err(GkPragmaError::INVALID_VALUE);
        }
        // BRANCH: mmap_size must be non-negative
        if let Some(v) = self.mmap_size
            && v < 0
        {
            return Err(GkPragmaError::INVALID_VALUE);
        }
        // BRANCH: wal_autocheckpoint range
        if let Some(v) = self.wal_autocheckpoint
            && !(0..=i64::from(i32::MAX)).contains(&v)
        {
            return Err(GkPragmaError::INVALID_VALUE);
        }
        // BRANCH: journal_size_limit lower bound
        if let Some(v) = self.journal_size_limit
            && v < -1
        {
            return Err(GkPragmaError::INVALID_VALUE);
        }
        // BRANCH: check every extra
        for (name, value) in &self.extra {
            if !valid_name(name) {
                // BRANCH: malformed extra name
                return Err(GkPragmaError::INVALID_NAME);
            }
            if TYPED_NAMES.iter().any(|t| t.eq_ignore_ascii_case(name)) {
                // BRANCH: extra shadows a typed field
                return Err(GkPragmaError::FORBIDDEN);
            }
            if let GkPragmaValue::Text(text) = value
                && !valid_text(text)
            {
                // BRANCH: unsafe extra text value
                return Err(GkPragmaError::INVALID_VALUE);
            }
        }
        Ok(())
    }

    /// Validates, then applies the pragmas to `conn` in a fixed order: `journal_mode`, `synchronous`,
    /// `locking_mode`, `temp_store`, `cache_size`, `mmap_size`, `wal_autocheckpoint`, `journal_size_limit`,
    /// `foreign_keys`, `query_only`, then the extras. A validation failure is `InvalidInput`.
    pub fn apply(&self, conn: &Connection) -> Result<(), GkSqliteErrorEnum> {
        self.validate()
            .map_err(|err| GkSqliteErrorEnum::InvalidInput(err.message()))?;
        self.for_each_step(|name, arg| {
            // BRANCH: integer or keyword argument
            match arg {
                GkPragmaArg::Int(v) => conn.pragma_update(None, name, v),
                GkPragmaArg::Text(v) => conn.pragma_update(None, name, v),
            }
            .map_err(GkSqliteErrorEnum::from)
        })
    }

    /// The `PRAGMA name=value` statements `apply` runs, in order. Meant for logging and tests.
    pub fn to_sql_statements(&self) -> Result<Vec<String>, GkPragmaError> {
        self.validate()?;
        let mut out = Vec::new();
        let _: Result<(), ()> = self.for_each_step(|name, arg| {
            // BRANCH: integer or keyword argument
            out.push(match arg {
                GkPragmaArg::Int(v) => format!("PRAGMA {name}={v}"),
                GkPragmaArg::Text(v) => format!("PRAGMA {name}={v}"),
            });
            Ok(())
        });
        Ok(out)
    }

    /// The single source of pragma order, shared by `apply` and `to_sql_statements`.
    fn for_each_step<E>(
        &self,
        mut f: impl FnMut(&str, GkPragmaArg<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        // BRANCH: emit journal_mode only when set
        if let Some(v) = self.journal_mode {
            f("journal_mode", GkPragmaArg::Text(v.as_str()))?;
        }
        // BRANCH: emit synchronous only when set
        if let Some(v) = self.synchronous {
            f("synchronous", GkPragmaArg::Text(v.as_str()))?;
        }
        // BRANCH: emit locking_mode only when set
        if let Some(v) = self.locking_mode {
            f("locking_mode", GkPragmaArg::Text(v.as_str()))?;
        }
        // BRANCH: emit temp_store only when set
        if let Some(v) = self.temp_store {
            f("temp_store", GkPragmaArg::Text(v.as_str()))?;
        }
        // BRANCH: emit cache_size_kib only when set
        if let Some(v) = self.cache_size_kib {
            f("cache_size", GkPragmaArg::Int(-v))?;
        }
        // BRANCH: emit mmap_size only when set
        if let Some(v) = self.mmap_size {
            f("mmap_size", GkPragmaArg::Int(v))?;
        }
        // BRANCH: emit wal_autocheckpoint only when set
        if let Some(v) = self.wal_autocheckpoint {
            f("wal_autocheckpoint", GkPragmaArg::Int(v))?;
        }
        // BRANCH: emit journal_size_limit only when set
        if let Some(v) = self.journal_size_limit {
            f("journal_size_limit", GkPragmaArg::Int(v))?;
        }
        // BRANCH: emit foreign_keys only when set
        if let Some(v) = self.foreign_keys {
            f("foreign_keys", GkPragmaArg::Int(i64::from(v)))?;
        }
        // BRANCH: emit query_only only when set
        if let Some(v) = self.query_only {
            f("query_only", GkPragmaArg::Int(i64::from(v)))?;
        }
        // BRANCH: extras last, in insertion order
        for (name, value) in &self.extra {
            // BRANCH: emit the extra by value kind
            match value {
                GkPragmaValue::Int(v) => f(name, GkPragmaArg::Int(*v))?,
                GkPragmaValue::Bool(v) => f(name, GkPragmaArg::Int(i64::from(*v)))?,
                GkPragmaValue::Text(v) => f(name, GkPragmaArg::Text(v))?,
            }
        }
        Ok(())
    }
}

/// `[A-Za-z_][A-Za-z0-9_]*`, 1..=64 bytes.
fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    // BRANCH: length bound and first character
    if bytes.is_empty() || bytes.len() > MAX_TOKEN_LEN {
        return false;
    }
    if !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_') {
        return false;
    }
    bytes[1..]
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
}

/// `[A-Za-z0-9_.-]+`, 1..=64 bytes.
fn valid_text(text: &str) -> bool {
    let bytes = text.as_bytes();
    // BRANCH: length bound and allowed alphabet
    if bytes.is_empty() || bytes.len() > MAX_TOKEN_LEN {
        return false;
    }
    bytes
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || matches!(*b, b'_' | b'.' | b'-'))
}
