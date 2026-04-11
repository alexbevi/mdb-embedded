//! Profiling, operation tracking, and top stats for the wire protocol.
//!
//! `OperationTracker` -- global registry of in-flight operations.
//! `TopStats` -- per-namespace timing stats for the `top` command.
//! `Profiler` -- ring-buffer profiler mirroring `db.setProfilingLevel()`.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use pyo3::prelude::*;
use pyo3::types::PyDict;

// ── OpEntry ─────────────────────────────────────────────────────────

/// One in-flight operation visible to Python (op id, ns, command, connection, timing).
#[pyclass(module = "smongo._smongo_core")]
pub struct OpEntry {
    #[pyo3(get)]
    pub op_id: i64,
    #[pyo3(get)]
    pub op: String,
    #[pyo3(get)]
    pub ns: String,
    #[pyo3(get)]
    pub command: Py<PyDict>,
    #[pyo3(get)]
    pub connection_id: i64,
    #[pyo3(get, set)]
    pub cancelled: bool,
    _start: Instant,
    #[pyo3(get)]
    pub start_time: f64,
}

// ── OperationTracker ────────────────────────────────────────────────

struct TrackedOp {
    op_id: i64,
    op: String,
    ns: String,
    command: Py<PyDict>,
    connection_id: i64,
    cancelled: bool,
    start: Instant,
}

/// Global registry of active operations; assigns op ids and supports killOp-style cancel.
#[pyclass(module = "smongo._smongo_core")]
pub struct OperationTracker {
    ops: Mutex<HashMap<i64, TrackedOp>>,
    counter: AtomicI64,
}

impl Default for OperationTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl OperationTracker {
    #[new]
    pub fn new() -> Self {
        Self {
            ops: Mutex::new(HashMap::new()),
            counter: AtomicI64::new(1),
        }
    }

    fn start_op(&self, op: &str, ns: &str, command: Py<PyDict>, connection_id: i64) -> i64 {
        let op_id = self.counter.fetch_add(1, Ordering::Relaxed);
        let entry = TrackedOp {
            op_id,
            op: op.to_string(),
            ns: ns.to_string(),
            command,
            connection_id,
            cancelled: false,
            start: Instant::now(),
        };
        self.ops.lock().insert(op_id, entry);
        op_id
    }

    fn finish_op(&self, op_id: i64) {
        self.ops.lock().remove(&op_id);
    }

    fn kill_op(&self, op_id: i64) -> bool {
        let mut ops = self.ops.lock();
        match ops.get_mut(&op_id) {
            Some(e) => {
                e.cancelled = true;
                true
            }
            None => false,
        }
    }

    fn active_ops(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let now = Instant::now();
        let ops = self.ops.lock();
        let mut result = Vec::with_capacity(ops.len());
        for e in ops.values() {
            let elapsed = now.duration_since(e.start);
            let d = PyDict::new(py);
            d.set_item("opid", e.op_id)?;
            d.set_item("active", true)?;
            d.set_item("op", &e.op)?;
            d.set_item("ns", &e.ns)?;
            d.set_item("command", e.command.bind(py))?;
            d.set_item("connectionId", e.connection_id)?;
            d.set_item("secs_running", elapsed.as_secs() as i64)?;
            d.set_item("microsecs_running", elapsed.as_micros() as i64)?;
            result.push(d.unbind());
        }
        Ok(result)
    }
}

// ── CollectionTimingStats (internal) ────────────────────────────────

struct TimingBucket {
    time: i64,
    count: i64,
}

impl TimingBucket {
    fn new() -> Self {
        Self { time: 0, count: 0 }
    }

    fn to_py_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        d.set_item("time", self.time)?;
        d.set_item("count", self.count)?;
        Ok(d.unbind())
    }
}

struct CollectionTimingStats {
    total: TimingBucket,
    read_lock: TimingBucket,
    write_lock: TimingBucket,
    queries: TimingBucket,
    getmore: TimingBucket,
    insert: TimingBucket,
    update: TimingBucket,
    remove: TimingBucket,
    commands: TimingBucket,
}

impl CollectionTimingStats {
    fn new() -> Self {
        Self {
            total: TimingBucket::new(),
            read_lock: TimingBucket::new(),
            write_lock: TimingBucket::new(),
            queries: TimingBucket::new(),
            getmore: TimingBucket::new(),
            insert: TimingBucket::new(),
            update: TimingBucket::new(),
            remove: TimingBucket::new(),
            commands: TimingBucket::new(),
        }
    }

    fn bucket_mut(&mut self, op: &str) -> &mut TimingBucket {
        match op {
            "queries" => &mut self.queries,
            "getmore" => &mut self.getmore,
            "insert" => &mut self.insert,
            "update" => &mut self.update,
            "remove" => &mut self.remove,
            "commands" => &mut self.commands,
            "total" => &mut self.total,
            "readLock" => &mut self.read_lock,
            "writeLock" => &mut self.write_lock,
            _ => &mut self.commands,
        }
    }

    fn record(&mut self, op: &str, micros: i64) {
        let bucket = self.bucket_mut(op);
        bucket.time += micros;
        bucket.count += 1;
        self.total.time += micros;
        self.total.count += 1;
        match op {
            "queries" | "getmore" => {
                self.read_lock.time += micros;
                self.read_lock.count += 1;
            }
            "insert" | "update" | "remove" => {
                self.write_lock.time += micros;
                self.write_lock.count += 1;
            }
            _ => {}
        }
    }

    fn to_py_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        d.set_item("total", self.total.to_py_dict(py)?)?;
        d.set_item("readLock", self.read_lock.to_py_dict(py)?)?;
        d.set_item("writeLock", self.write_lock.to_py_dict(py)?)?;
        d.set_item("queries", self.queries.to_py_dict(py)?)?;
        d.set_item("getmore", self.getmore.to_py_dict(py)?)?;
        d.set_item("insert", self.insert.to_py_dict(py)?)?;
        d.set_item("update", self.update.to_py_dict(py)?)?;
        d.set_item("remove", self.remove.to_py_dict(py)?)?;
        d.set_item("commands", self.commands.to_py_dict(py)?)?;
        Ok(d.unbind())
    }
}

// ── TopStats ────────────────────────────────────────────────────────

/// Per-namespace microsecond aggregates for MongoDB `top`-style server stats.
#[pyclass(module = "smongo._smongo_core")]
pub struct TopStats {
    stats: Mutex<HashMap<String, CollectionTimingStats>>,
}

impl Default for TopStats {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl TopStats {
    #[new]
    pub fn new() -> Self {
        Self {
            stats: Mutex::new(HashMap::new()),
        }
    }

    fn record(&self, ns: &str, op: &str, micros: i64) {
        let mut stats = self.stats.lock();
        stats
            .entry(ns.to_string())
            .or_insert_with(CollectionTimingStats::new)
            .record(op, micros);
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let stats = self.stats.lock();
        let d = PyDict::new(py);
        for (ns, s) in stats.iter() {
            d.set_item(ns, s.to_py_dict(py)?)?;
        }
        Ok(d.unbind())
    }
}

// ── Profiler ────────────────────────────────────────────────────────

/// Ring-buffer slow-query profiler mirroring `db.setProfilingLevel()` behavior.
#[pyclass(module = "smongo._smongo_core")]
pub struct Profiler {
    #[pyo3(get, set)]
    pub level: i32,
    #[pyo3(get, set)]
    pub slow_ms: i64,
    entries: Mutex<Vec<Py<PyDict>>>,
    max_entries: usize,
}

#[pymethods]
impl Profiler {
    #[new]
    #[pyo3(signature = (level=0, slow_ms=100, max_entries=4096))]
    pub fn new(level: i32, slow_ms: i64, max_entries: usize) -> Self {
        Self {
            level,
            slow_ms,
            entries: Mutex::new(Vec::new()),
            max_entries,
        }
    }

    #[pyo3(signature = (op, ns, millis, command=None, plan_summary=String::new(), response_length=0, n_returned=0))]
    #[allow(clippy::too_many_arguments)]
    fn log(
        &self,
        py: Python<'_>,
        op: &str,
        ns: &str,
        millis: i64,
        command: Option<&Bound<'_, PyDict>>,
        plan_summary: String,
        response_length: i64,
        n_returned: i64,
    ) -> PyResult<()> {
        if self.level == 0 {
            return Ok(());
        }
        if self.level == 1 && millis < self.slow_ms {
            return Ok(());
        }

        let datetime_mod = crate::cached_modules::datetime(py)?;
        let utc = datetime_mod.getattr("timezone")?.getattr("utc")?;
        let now = datetime_mod
            .getattr("datetime")?
            .call_method1("now", (&utc,))?;

        let entry = PyDict::new(py);
        entry.set_item("op", op)?;
        entry.set_item("ns", ns)?;
        entry.set_item("millis", millis)?;
        entry.set_item("ts", now)?;
        let cmd = match command {
            Some(c) => c.clone().into_any(),
            None => PyDict::new(py).into_any(),
        };
        entry.set_item("command", cmd)?;
        entry.set_item("planSummary", plan_summary)?;
        entry.set_item("responseLength", response_length)?;
        entry.set_item("nreturned", n_returned)?;

        let mut entries = self.entries.lock();
        entries.push(entry.unbind());
        if entries.len() > self.max_entries {
            let drain_count = entries.len() - self.max_entries;
            entries.drain(..drain_count);
        }
        Ok(())
    }

    #[pyo3(signature = (limit=100))]
    fn get_entries(&self, py: Python<'_>, limit: usize) -> Vec<Py<PyDict>> {
        let entries = self.entries.lock();
        let start = entries.len().saturating_sub(limit);
        entries[start..].iter().map(|e| e.clone_ref(py)).collect()
    }

    fn clear(&self) {
        self.entries.lock().clear();
    }
}
