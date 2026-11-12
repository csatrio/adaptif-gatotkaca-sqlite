// ============================================================================
// Import Definition
// ============================================================================

use adaptif_gatotkaca_sqlite::{
    GkCompactStr, GkSavepointAction, GkSqliteConfig, GkSqliteDb, GkSqliteErrorEnum, GkSqliteParams,
    GkSqliteRowRecord, GkSqliteValue,
};
use pyo3::{
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple},
};
use std::time::Duration;

// ============================================================================
// Conversions
// ============================================================================

/// The engine's errors reach Python as `RuntimeError("SQLite error: ...")`; `sqlite.sqlite.translate` maps them to `sqlite3` classes.
fn to_py_err(err: GkSqliteErrorEnum) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

fn to_value(obj: &Bound<'_, PyAny>) -> PyResult<GkSqliteValue> {
    // BRANCH: None
    if obj.is_none() {
        return Ok(GkSqliteValue::Null);
    }
    // BRANCH: bool is an int subclass, checked first
    if let Ok(b) = obj.cast::<PyBool>() {
        return Ok(GkSqliteValue::Integer(i64::from(b.is_true())));
    }
    // BRANCH: int
    if obj.is_instance_of::<PyInt>() {
        return Ok(GkSqliteValue::Integer(obj.extract::<i64>()?));
    }
    // BRANCH: float
    if obj.is_instance_of::<PyFloat>() {
        return Ok(GkSqliteValue::Real(obj.extract::<f64>()?));
    }
    // BRANCH: str
    if let Ok(s) = obj.cast::<PyString>() {
        return Ok(GkSqliteValue::Text(GkCompactStr::new(s.to_str()?)));
    }
    // BRANCH: bytes
    if let Ok(b) = obj.cast::<PyBytes>() {
        return Ok(GkSqliteValue::Blob(b.as_bytes().into()));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(format!(
        "unsupported SQL parameter type: {}",
        obj.get_type().name()?
    )))
}

fn to_params(params: Option<&Bound<'_, PyAny>>) -> PyResult<GkSqliteParams> {
    let mut out: Vec<GkSqliteValue> = Vec::new();
    // BRANCH: no parameters
    let Some(params) = params else {
        return Ok(GkSqliteParams::from(out));
    };
    if params.is_none() {
        return Ok(GkSqliteParams::from(out));
    }
    for item in params.try_iter()? {
        out.push(to_value(&item?)?);
    }
    Ok(GkSqliteParams::from(out))
}

fn from_value<'py>(py: Python<'py>, v: &GkSqliteValue) -> PyResult<Bound<'py, PyAny>> {
    // BRANCH: one arm per storage class
    Ok(match v {
        GkSqliteValue::Null => py.None().into_bound(py),
        GkSqliteValue::Integer(n) => n.into_pyobject(py)?.into_any(),
        GkSqliteValue::Real(r) => r.into_pyobject(py)?.into_any(),
        GkSqliteValue::Text(s) => PyString::new(py, s.as_str()).into_any(),
        GkSqliteValue::Blob(b) => PyBytes::new(py, b).into_any(),
    })
}

fn tuples<'py>(py: Python<'py>, rows: &[GkSqliteRowRecord]) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for row in rows {
        let items = row
            .values
            .iter()
            .map(|v| from_value(py, v))
            .collect::<PyResult<Vec<_>>>()?;
        out.append(PyTuple::new(py, items)?)?;
    }
    Ok(out)
}

fn dicts<'py>(py: Python<'py>, rows: &[GkSqliteRowRecord]) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for row in rows {
        let d = PyDict::new(py);
        for (name, v) in row.column_names.iter().zip(row.values.iter()) {
            d.set_item(&**name, from_value(py, v)?)?;
        }
        out.append(d)?;
    }
    Ok(out)
}

fn batch_params(batch: &Bound<'_, PyAny>) -> PyResult<Vec<GkSqliteParams>> {
    let mut out = Vec::new();
    for item in batch.try_iter()? {
        out.push(to_params(Some(&item?))?);
    }
    Ok(out)
}

fn savepoint(verb: &str, name: &str) -> GkSavepointAction {
    let name = GkCompactStr::new(name);
    // BRANCH: savepoint verb
    match verb {
        "savepoint" => GkSavepointAction::Savepoint(name),
        "rollback_to" => GkSavepointAction::RollbackTo(name),
        _ => GkSavepointAction::Release(name),
    }
}

// ============================================================================
// Engine and transaction objects (synchronous; the Python layer adds the async calls)
// ============================================================================

#[pyclass(name = "PySqliteDb", module = "_sqlite_engine")]
struct PySqliteDb {
    db: Option<GkSqliteDb>,
}

impl PySqliteDb {
    fn db(&self) -> PyResult<&GkSqliteDb> {
        self.db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("SQLite error: engine already finalized"))
    }
}

#[pymethods]
impl PySqliteDb {
    #[pyo3(signature = (sql, parameters=None))]
    fn query_tuples_sync<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        parameters: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let (db, p) = (self.db()?.clone(), to_params(parameters)?);
        let rows = py.detach(|| db.query_records(sql, p)).map_err(to_py_err)?;
        tuples(py, &rows)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn query_sync<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        parameters: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let (db, p) = (self.db()?.clone(), to_params(parameters)?);
        let rows = py.detach(|| db.query_records(sql, p)).map_err(to_py_err)?;
        dicts(py, &rows)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute_sync(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<usize> {
        let (db, p) = (self.db()?.clone(), to_params(parameters)?);
        let out = py.detach(|| db.execute(sql, p)).map_err(to_py_err)?;
        Ok(out.rows)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute_insert_sync(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<i64> {
        let (db, p) = (self.db()?.clone(), to_params(parameters)?);
        py.detach(|| db.execute_insert(sql, p)).map_err(to_py_err)
    }

    fn execute_many_sync(
        &self,
        py: Python<'_>,
        sql: &str,
        batch: &Bound<'_, PyAny>,
    ) -> PyResult<usize> {
        let (db, p) = (self.db()?.clone(), batch_params(batch)?);
        let out = py.detach(|| db.execute_many(sql, p)).map_err(to_py_err)?;
        Ok(out.rows)
    }

    fn execute_batch_sync(&self, py: Python<'_>, sql: &str) -> PyResult<()> {
        let db = self.db()?.clone();
        py.detach(|| db.execute_batch(sql)).map_err(to_py_err)?;
        Ok(())
    }

    fn begin_transaction_sync(&self, py: Python<'_>) -> PyResult<PySqliteTransaction> {
        let db = self.db()?.clone();
        let session = py.detach(|| db.begin_session()).map_err(to_py_err)?;
        Ok(PySqliteTransaction {
            db: Some(db),
            session,
        })
    }

    fn last_committed(&self) -> PyResult<u64> {
        Ok(self.db()?.last_committed())
    }

    fn close_sync(&mut self, py: Python<'_>) {
        // The writer thread stops when the last clone (this one, or a live transaction's) is dropped.
        let db = self.db.take();
        py.detach(|| drop(db));
    }
}

#[pyclass(name = "PySqliteTransaction", module = "_sqlite_engine")]
struct PySqliteTransaction {
    db: Option<GkSqliteDb>,
    session: u64,
}

impl PySqliteTransaction {
    fn db(&self) -> PyResult<&GkSqliteDb> {
        self.db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("SQLite error: transaction already finalized"))
    }

    fn finish(&mut self, py: Python<'_>, commit: bool) -> PyResult<()> {
        let db = self.db.take().ok_or_else(|| {
            PyRuntimeError::new_err("SQLite error: transaction already finalized")
        })?;
        let session = self.session;
        py.detach(|| db.end_session(session, commit))
            .map_err(to_py_err)?;
        Ok(())
    }
}

#[pymethods]
impl PySqliteTransaction {
    #[pyo3(signature = (sql, parameters=None))]
    fn query_tuples_sync<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        parameters: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let (db, p, s) = (self.db()?.clone(), to_params(parameters)?, self.session);
        let rows = py
            .detach(|| db.session_query_records(s, sql, p))
            .map_err(to_py_err)?;
        tuples(py, &rows)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn query_sync<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        parameters: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let (db, p, s) = (self.db()?.clone(), to_params(parameters)?, self.session);
        let rows = py
            .detach(|| db.session_query_records(s, sql, p))
            .map_err(to_py_err)?;
        dicts(py, &rows)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute_sync(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<usize> {
        let (db, p, s) = (self.db()?.clone(), to_params(parameters)?, self.session);
        let out = py
            .detach(|| db.session_execute(s, sql, p))
            .map_err(to_py_err)?;
        Ok(out.rows)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute_insert_sync(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<i64> {
        let (db, p, s) = (self.db()?.clone(), to_params(parameters)?, self.session);
        py.detach(|| db.session_execute_insert(s, sql, p))
            .map_err(to_py_err)
    }

    fn execute_batch_sync(&self, py: Python<'_>, sql: &str) -> PyResult<()> {
        let (db, s) = (self.db()?.clone(), self.session);
        py.detach(|| db.session_execute_batch(s, sql))
            .map_err(to_py_err)?;
        Ok(())
    }

    fn savepoint_sync(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        self.run_savepoint(py, "savepoint", name)
    }

    fn rollback_to_sync(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        self.run_savepoint(py, "rollback_to", name)
    }

    fn release_savepoint_sync(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        self.run_savepoint(py, "release", name)
    }

    fn commit_sync(&mut self, py: Python<'_>) -> PyResult<()> {
        self.finish(py, true)
    }

    fn rollback_sync(&mut self, py: Python<'_>) -> PyResult<()> {
        self.finish(py, false)
    }

    /// Releases the lease; rolls back when the transaction is still open. Safe to call after commit / rollback.
    fn close_sync(&mut self, py: Python<'_>) -> PyResult<()> {
        // BRANCH: already finished
        if self.db.is_none() {
            return Ok(());
        }
        self.finish(py, false)
    }
}

impl PySqliteTransaction {
    fn run_savepoint(&self, py: Python<'_>, verb: &str, name: &str) -> PyResult<()> {
        let (db, s) = (self.db()?.clone(), self.session);
        let action = savepoint(verb, name);
        py.detach(|| db.session_savepoint(s, action))
            .map_err(to_py_err)
    }
}

// ============================================================================
// Module
// ============================================================================

/// Opens the engine for `path` (no pool: the engine is one writer thread plus thread-local readers).
/// `max_size` and `wal` are accepted for the main repo's signature; the engine always runs WAL.
#[pyfunction]
#[pyo3(signature = (path, max_size=None, wal=true, busy_timeout_ms=5000))]
fn create_pool(
    py: Python<'_>,
    path: &str,
    max_size: Option<usize>,
    wal: bool,
    busy_timeout_ms: u64,
) -> PyResult<PySqliteDb> {
    let _ = (max_size, wal);
    let config = GkSqliteConfig {
        busy_timeout: Duration::from_millis(busy_timeout_ms),
        ..GkSqliteConfig::default()
    };
    let db = py
        .detach(|| GkSqliteDb::open(path, config))
        .map_err(to_py_err)?;
    Ok(PySqliteDb { db: Some(db) })
}

#[pyfunction]
fn sqlite_version() -> &'static str {
    adaptif_gatotkaca_sqlite::rusqlite::version()
}

#[pymodule]
fn _sqlite_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySqliteDb>()?;
    m.add_class::<PySqliteTransaction>()?;
    m.add_function(wrap_pyfunction!(create_pool, m)?)?;
    m.add_function(wrap_pyfunction!(sqlite_version, m)?)?;
    Ok(())
}
