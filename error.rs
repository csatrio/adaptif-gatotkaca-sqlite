// ============================================================================
// Import Definition
// ============================================================================

use std::fmt;

// ============================================================================
// Error Definition
// ============================================================================

#[derive(Debug)]
pub enum GkSqliteErrorEnum {
    SqliteError(rusqlite::Error),
    WriterDead,
    UseTransactionApi,
    Timeout,
    NotAutocommit,
    InvalidInput(&'static str),
    /// The reader executor (pool) rejected or dropped a read job.
    ReaderUnavailable,
}

pub type GkSqliteError = GkSqliteErrorEnum;

impl fmt::Display for GkSqliteErrorEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // BRANCH: Format error variant
        match self {
            Self::SqliteError(err) => write!(f, "SQLite error: {}", err),
            Self::WriterDead => write!(f, "SQLite writer thread has disconnected or terminated"),
            Self::UseTransactionApi => write!(
                f,
                "Explicit transaction keywords (BEGIN, COMMIT, ROLLBACK, SAVEPOINT) are not allowed in execute(); use db.transaction() instead"
            ),
            Self::Timeout => write!(f, "Operation timed out"),
            Self::NotAutocommit => {
                write!(
                    f,
                    "Reader connection is unexpectedly not in autocommit mode"
                )
            }
            Self::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            Self::ReaderUnavailable => {
                write!(f, "The reader executor is shut down or dropped the job")
            }
        }
    }
}

impl std::error::Error for GkSqliteErrorEnum {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // BRANCH: Return underlying source if Sqlite variant
        match self {
            Self::SqliteError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for GkSqliteErrorEnum {
    fn from(err: rusqlite::Error) -> Self {
        Self::SqliteError(err)
    }
}
