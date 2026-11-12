// ============================================================================
// Import Definition
// ============================================================================

use crate::{
    error::GkSqliteErrorEnum,
    executor::{GkReaderExecutor, GkThreadOptions},
    pool::GkReaderPoolConfig,
    pragma::GkSqlitePragmas,
    queue::GkChannelKind,
};
use rusqlite::Connection;
use std::{fmt, sync::Arc, time::Duration};

// ============================================================================
// Connection Hook Definition
// ============================================================================

/// Which of the two connection kinds a hook is running for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GkConnectionRole {
    /// The connection owned by the dedicated writer thread.
    Writer,
    /// A thread-local read-only connection.
    Reader,
}

/// The shape of a connection hook.
type GkConnectionHookFn =
    Arc<dyn Fn(&Connection, GkConnectionRole) -> rusqlite::Result<()> + Send + Sync>;

/// Callback run once on every freshly opened connection, after the pragmas are applied.
/// Use it to register custom SQL functions, collations or extensions.
#[derive(Clone)]
pub struct GkConnectionHook(GkConnectionHookFn);

impl GkConnectionHook {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&Connection, GkConnectionRole) -> rusqlite::Result<()> + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    #[inline]
    pub fn call(&self, conn: &Connection, role: GkConnectionRole) -> rusqlite::Result<()> {
        (self.0)(conn, role)
    }
}

impl fmt::Debug for GkConnectionHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GkConnectionHook(..)")
    }
}

// ============================================================================
// Connection Options Definition
// ============================================================================

/// Everything needed to open and initialise one connection kind. Built from [`GkSqliteConfig`].
#[derive(Debug, Clone)]
pub struct GkConnectionOptions {
    pub role: GkConnectionRole,
    pub busy_timeout: Duration,
    pub pragmas: GkSqlitePragmas,
    pub hook: Option<GkConnectionHook>,
}

impl GkConnectionOptions {
    /// Runs the post-open initialisation shared by writer and reader connections.
    pub fn initialise(&self, conn: &Connection) -> Result<(), GkSqliteErrorEnum> {
        self.pragmas.apply(conn)?;
        conn.busy_timeout(self.busy_timeout)?;
        conn.set_prepared_statement_cache_capacity(crate::writer::STATEMENT_CACHE_CAPACITY);
        // BRANCH: user hook registered
        if let Some(hook) = &self.hook {
            hook.call(conn, self.role)?;
        }
        Ok(())
    }
}

// ============================================================================
// Read Mode Definition
// ============================================================================

/// Where the `*_async` / `read` methods run their reads. The synchronous `query_*` methods always run
/// inline on the calling thread's own connection.
#[derive(Clone, Default)]
pub enum GkReadMode {
    /// Run on the calling thread (no handoff).
    #[default]
    Inline,
    /// A built-in lock-free pool of reader threads, each with its own connection.
    Pool(GkReaderPoolConfig),
    /// The caller's own pool or runtime (see [`GkReaderExecutor`]).
    Custom(Arc<dyn GkReaderExecutor>),
}

impl fmt::Debug for GkReadMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // BRANCH: describe the mode without requiring Debug of a custom executor
        match self {
            Self::Inline => f.write_str("Inline"),
            Self::Pool(cfg) => f.debug_tuple("Pool").field(cfg).finish(),
            Self::Custom(exec) => f.debug_tuple("Custom").field(&exec.name()).finish(),
        }
    }
}

// ============================================================================
// Configuration Definition
// ============================================================================

#[derive(Debug, Clone)]
pub struct GkSqliteConfig {
    pub busy_timeout: Duration,
    /// Capacity of the writer queue (per producing thread for the Treiber channel).
    pub writer_queue_size: usize,
    /// Which channel carries writes to the writer thread (default: the gatotkaca Treiber channel).
    pub writer_channel: GkChannelKind,
    pub read_timeout: Duration,
    pub autocommit_mode: bool,
    /// Pragmas applied to the writer connection (default: WAL + `synchronous=NORMAL`).
    pub writer_pragmas: GkSqlitePragmas,
    /// Pragmas applied to every reader connection (default: `query_only`, 256 MB mmap, 64 MB cache).
    pub reader_pragmas: GkSqlitePragmas,
    /// Runs on every new connection after its pragmas.
    pub on_connection_open: Option<GkConnectionHook>,
    /// Where `*_async` reads run (default: inline).
    pub read_mode: GkReadMode,
    /// Name, stack size and start/stop hooks of the writer thread.
    pub writer_thread: GkThreadOptions,
}

impl Default for GkSqliteConfig {
    fn default() -> Self {
        Self {
            busy_timeout: Duration::from_secs(5),
            writer_queue_size: 1024,
            writer_channel: GkChannelKind::Treiber,
            read_timeout: Duration::from_millis(5),
            autocommit_mode: true,
            writer_pragmas: GkSqlitePragmas::writer_default(),
            reader_pragmas: GkSqlitePragmas::reader_default(),
            on_connection_open: None,
            read_mode: GkReadMode::Inline,
            writer_thread: GkThreadOptions::default().named("sqlite-writer"),
        }
    }
}

impl GkSqliteConfig {
    /// Starts a builder from the defaults.
    pub fn builder() -> GkSqliteConfigBuilder {
        GkSqliteConfigBuilder {
            config: Self::default(),
        }
    }

    /// Favours write throughput: bigger writer cache, in-memory temp store, rarer checkpoints.
    pub fn throughput() -> Self {
        Self {
            writer_pragmas: GkSqlitePragmas::writer_throughput(),
            ..Self::default()
        }
    }

    /// Favours durability: `synchronous=FULL` on the writer.
    pub fn durable() -> Self {
        Self {
            writer_pragmas: GkSqlitePragmas::writer_durable(),
            ..Self::default()
        }
    }

    /// Favours a small footprint: 2 MB reader caches and no mmap.
    pub fn low_memory() -> Self {
        Self {
            reader_pragmas: GkSqlitePragmas::reader_low_memory(),
            ..Self::default()
        }
    }

    /// Checks the pieces that can be wrong before any thread or file is touched.
    pub fn validate(&self) -> Result<(), GkSqliteErrorEnum> {
        // BRANCH: zero queue size cannot hold a message
        if self.writer_queue_size == 0 {
            return Err(GkSqliteErrorEnum::InvalidInput(
                "writer_queue_size must be at least 1",
            ));
        }
        self.writer_pragmas
            .validate()
            .map_err(|e| GkSqliteErrorEnum::InvalidInput(e.message()))?;
        self.reader_pragmas
            .validate()
            .map_err(|e| GkSqliteErrorEnum::InvalidInput(e.message()))?;
        // BRANCH: a built-in pool config must be valid
        if let GkReadMode::Pool(pool) = &self.read_mode {
            pool.validate()
                .map_err(|_| GkSqliteErrorEnum::InvalidInput("invalid reader pool config"))?;
        }
        Ok(())
    }

    pub(crate) fn writer_options(&self) -> GkConnectionOptions {
        GkConnectionOptions {
            role: GkConnectionRole::Writer,
            busy_timeout: self.busy_timeout,
            pragmas: self.writer_pragmas.clone(),
            hook: self.on_connection_open.clone(),
        }
    }

    pub(crate) fn reader_options(&self) -> GkConnectionOptions {
        GkConnectionOptions {
            role: GkConnectionRole::Reader,
            busy_timeout: self.busy_timeout,
            pragmas: self.reader_pragmas.clone(),
            hook: self.on_connection_open.clone(),
        }
    }
}

// ============================================================================
// Configuration Builder Definition
// ============================================================================

/// Chainable construction of a [`GkSqliteConfig`]; `build()` validates it.
#[derive(Debug, Clone)]
pub struct GkSqliteConfigBuilder {
    config: GkSqliteConfig,
}

impl GkSqliteConfigBuilder {
    pub fn from_config(config: GkSqliteConfig) -> Self {
        Self { config }
    }

    pub fn busy_timeout(mut self, v: Duration) -> Self {
        self.config.busy_timeout = v;
        self
    }

    pub fn writer_queue_size(mut self, v: usize) -> Self {
        self.config.writer_queue_size = v;
        self
    }

    pub fn writer_channel(mut self, v: GkChannelKind) -> Self {
        self.config.writer_channel = v;
        self
    }

    pub fn read_timeout(mut self, v: Duration) -> Self {
        self.config.read_timeout = v;
        self
    }

    pub fn autocommit_mode(mut self, v: bool) -> Self {
        self.config.autocommit_mode = v;
        self
    }

    pub fn writer_pragmas(mut self, v: GkSqlitePragmas) -> Self {
        self.config.writer_pragmas = v;
        self
    }

    pub fn reader_pragmas(mut self, v: GkSqlitePragmas) -> Self {
        self.config.reader_pragmas = v;
        self
    }

    pub fn on_connection_open<F>(mut self, f: F) -> Self
    where
        F: Fn(&Connection, GkConnectionRole) -> rusqlite::Result<()> + Send + Sync + 'static,
    {
        self.config.on_connection_open = Some(GkConnectionHook::new(f));
        self
    }

    pub fn read_mode(mut self, v: GkReadMode) -> Self {
        self.config.read_mode = v;
        self
    }

    /// Shorthand for `read_mode(GkReadMode::Pool(config))`.
    pub fn reader_pool(mut self, v: GkReaderPoolConfig) -> Self {
        self.config.read_mode = GkReadMode::Pool(v);
        self
    }

    pub fn writer_thread(mut self, v: GkThreadOptions) -> Self {
        self.config.writer_thread = v;
        self
    }

    pub fn build(self) -> Result<GkSqliteConfig, GkSqliteErrorEnum> {
        self.config.validate()?;
        Ok(self.config)
    }
}
