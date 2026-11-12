// ============================================================================
// Import Definition
// ============================================================================

use std::{ops::Deref, sync::Arc};

// ============================================================================
// SQL Text Definition
// ============================================================================

/// SQL text handed to the engine. A `&'static str` is carried as is (no copy, no allocation); anything
/// else is shared through one `Arc<str>`. Cloning is at most a reference-count increment.
#[derive(Debug, Clone)]
pub enum GkSqlText {
    /// Constant SQL: zero allocation.
    Static(&'static str),
    /// Heap SQL, shared between the caller, the queue and the statement lookups.
    Shared(Arc<str>),
}

impl GkSqlText {
    /// Copies a borrowed `&str` into shared storage (the one allocation a non-static caller pays).
    #[inline]
    pub fn from_copy(sql: &str) -> Self {
        Self::Shared(Arc::from(sql))
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        // BRANCH: static or shared storage
        match self {
            Self::Static(s) => s,
            Self::Shared(s) => s,
        }
    }
}

impl Deref for GkSqlText {
    type Target = str;
    #[inline]
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl From<&'static str> for GkSqlText {
    #[inline]
    fn from(sql: &'static str) -> Self {
        Self::Static(sql)
    }
}

impl From<String> for GkSqlText {
    #[inline]
    fn from(sql: String) -> Self {
        Self::Shared(Arc::from(sql))
    }
}

impl From<Arc<str>> for GkSqlText {
    #[inline]
    fn from(sql: Arc<str>) -> Self {
        Self::Shared(sql)
    }
}
