//! Rust port of `smongo.storage.streaming.StreamingCursor`.
//!
//! Provides a lazy Python iterator over WiredTiger documents with
//! direct WtCursor access.  The query planner is consulted once
//! in `__iter__` to choose the scan strategy, then `__next__`
//! advances through results one at a time.

use std::mem::ManuallyDrop;
use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};

use crate::bson_helpers;
use crate::wt_bridge::WtResultExt;
use crate::locking::{InlineRwLock, MutexForceGuard};
use crate::query_compiler;
use crate::wt_safe::{WtCursor, WtSession};

enum IterState {
    NotStarted,
    FullScan {
        cursor: WtCursor,
    },
    IdScan {
        cursor: WtCursor,
        ids: Vec<String>,
        idx: usize,
    },
    /// Precomputed `$near` / 2dsphere results in distance order.
    GeoNearOrdered {
        docs: Vec<Py<PyDict>>,
        idx: usize,
    },
    SingleDoc {
        doc: Option<Py<PyDict>>,
    },
    Exhausted,
}

/// Lazy WiredTiger cursor that yields documents one at a time.
#[pyclass]
pub struct RustStreamingCursor {
    /// Collection handle for engine-backed geo / `$or` materialized finds.
    parent_coll: Py<crate::local_collection::RustLocalCollection>,
    table_uri: crate::TableUri,
    query: Py<PyDict>,
    rwlock: Arc<InlineRwLock>,
    mutex: Arc<Mutex<()>>,
    planner: Py<crate::query_planner::RustQueryPlanner>,
    #[allow(dead_code)] // Keeps the Python session alive so session_raw remains valid.
    session_py: Py<PyAny>,
    session_raw: Option<*mut wiredtiger_sys::WT_SESSION>,

    state: IterState,
    mutex_guard: Option<MutexForceGuard>,
}

// SAFETY: RustStreamingCursor is a #[pyclass] requiring Send+Sync.  The
// session_raw pointer is only dereferenced while the collection's InlineRwLock
// (read lock) and Mutex are held, providing serialization independent of GIL
// state.  Cursors are single-consumer iterators -- never shared across threads.
// Under free-threaded Python, PyO3's borrow checking on __next__(&mut self)
// prevents concurrent iteration.
unsafe impl Send for RustStreamingCursor {}
unsafe impl Sync for RustStreamingCursor {}

impl RustStreamingCursor {
    fn borrow_session(&self) -> PyResult<ManuallyDrop<WtSession>> {
        crate::wt_bridge::borrow_wt_session(self.session_raw, "streaming cursor")
    }

    fn acquire_locks(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.mutex_guard.is_none() {
            let rwlock = Arc::clone(&self.rwlock);
            py.detach(|| rwlock.acquire_read());
            self.mutex_guard = Some(MutexForceGuard::acquire(py, &self.mutex));
        }
        Ok(())
    }

    fn release_locks(&mut self, _py: Python<'_>) {
        if self.mutex_guard.is_some() {
            self.mutex_guard = None;
            self.rwlock.release_read();
        }
    }

    pub(crate) fn from_collection(
        py: Python<'_>,
        coll: Py<crate::local_collection::RustLocalCollection>,
        query: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<Self>> {
        let q = match query {
            Some(q) => {
                if let Ok(d) = q.cast::<PyDict>() {
                    d.copy()?.unbind()
                } else {
                    PyDict::new(py).unbind()
                }
            }
            None => PyDict::new(py).unbind(),
        };

        let (table_uri, rwlock, mutex, planner, session_py_coll) = {
            let b = coll.bind(py).borrow();
            (
                b.table_uri.clone(),
                Arc::clone(&b.rwlock),
                Arc::clone(&b.mutex),
                b.planner.clone_ref(py),
                b.session_py.clone_ref(py),
            )
        };

        let session_py: Py<PyAny> = match crate::transaction::get_active_txn_session(py) {
            Ok(Some(s)) => s,
            _ => session_py_coll.into_any(),
        };

        let session_raw = {
            let sess_obj = session_py.bind(py);
            if let Ok(rs) = sess_obj.extract::<Py<crate::wt_bridge::RustWtSession>>() {
                let bound = rs.bind(py).borrow();
                bound.get().py()?.raw_ptr()
            } else {
                return Err(PyRuntimeError::new_err(
                    "StreamingCursor requires RustWtSession",
                ));
            }
        };

        Py::new(
            py,
            Self {
                parent_coll: coll.clone_ref(py),
                table_uri,
                query: q,
                rwlock,
                mutex,
                planner,
                session_py,
                session_raw: Some(session_raw),
                state: IterState::NotStarted,
                mutex_guard: None,
            },
        )
    }

    fn init_state(&mut self, py: Python<'_>) -> PyResult<()> {
        self.acquire_locks(py)?;

        let query_ref = self.query.clone_ref(py);
        let query = query_ref.bind(py);
        let plan = self.planner.bind(py).borrow().plan(py, query)?;

        match plan.plan_type {
            crate::query_planner::PlanType::PkLookup => self.init_pk_lookup(py, query)?,
            crate::query_planner::PlanType::IndexScan => {
                self.init_index_scan_typed(py, query, &plan)?
            }
            crate::query_planner::PlanType::GeoNear
            | crate::query_planner::PlanType::GeoWithin
            | crate::query_planner::PlanType::GeoIntersects => {
                self.init_engine_materialized_find(py, query)?
            }
            crate::query_planner::PlanType::OrUnion if plan.subplans.is_some() => {
                self.init_engine_materialized_find(py, query)?;
            }
            _ => self.init_full_scan()?,
        }
        Ok(())
    }

    fn init_pk_lookup(&mut self, py: Python<'_>, query: &Bound<'_, PyDict>) -> PyResult<()> {
        let raw_id = match query.get_item("_id")? {
            Some(v) => v,
            None => {
                self.state = IterState::SingleDoc { doc: None };
                return Ok(());
            }
        };
        let pk_val = if let Ok(d) = raw_id.cast::<PyDict>() {
            if let Some(eq) = d.get_item("$eq")? {
                eq.str()?.to_string()
            } else {
                raw_id.str()?.to_string()
            }
        } else {
            raw_id.str()?.to_string()
        };

        let session = self.borrow_session()?;
        let mut cursor = session.open_cursor(&self.table_uri, None).py()?;
        cursor.set_key_str(&pk_val);
        let doc = if cursor.search().is_ok() {
            let raw = cursor.get_value_raw().py()?;
            let d = bson_helpers::from_bson(py, &raw)?;

            let remaining = PyDict::new(py);
            for (k, v) in query.iter() {
                let ks: String = k.extract()?;
                if ks != "_id" {
                    remaining.set_item(k, v)?;
                }
            }
            if !remaining.is_empty() {
                if query_compiler::eval_query(&d, &remaining)? {
                    Some(d.unbind())
                } else {
                    None
                }
            } else {
                Some(d.unbind())
            }
        } else {
            None
        };
        cursor.close().py()?;
        self.state = IterState::SingleDoc { doc };
        Ok(())
    }

    fn init_index_scan_typed(
        &mut self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
        plan: &crate::query_planner::QueryPlan,
    ) -> PyResult<()> {
        let ids = if let Some(ref idx_name) = plan.index_def_name {
            let mgr_py = {
                let planner = self.planner.bind(py).borrow();
                planner.index_mgr_ref(py)
            };
            let mgr = mgr_py.bind(py).borrow();
            let leading = mgr
                .indexes()
                .get(idx_name)
                .and_then(|idx| idx.keys.first().map(|(f, _)| f.clone()));
            drop(mgr);

            if let Some(leading_field) = leading {
                if let Some(cond) = query.get_item(&leading_field)? {
                    if let Ok(cd) = cond.cast::<PyDict>() {
                        if let Some(in_vals) = cd.get_item("$in")? {
                            self.planner.bind(py).borrow().execute_in_scan(
                                py,
                                idx_name,
                                &in_vals,
                                self.session_raw,
                            )?
                        } else {
                            self.planner.bind(py).borrow().execute_index_scan(
                                py,
                                plan,
                                self.session_raw,
                            )?
                        }
                    } else {
                        self.planner.bind(py).borrow().execute_index_scan(
                            py,
                            plan,
                            self.session_raw,
                        )?
                    }
                } else {
                    self.planner
                        .bind(py)
                        .borrow()
                        .execute_index_scan(py, plan, self.session_raw)?
                }
            } else {
                self.planner
                    .bind(py)
                    .borrow()
                    .execute_index_scan(py, plan, self.session_raw)?
            }
        } else {
            self.planner
                .bind(py)
                .borrow()
                .execute_index_scan(py, plan, self.session_raw)?
        };

        let session = self.borrow_session()?;
        let cursor = session.open_cursor(&self.table_uri, None).py()?;
        self.state = IterState::IdScan {
            cursor,
            ids,
            idx: 0,
        };
        Ok(())
    }

    fn init_engine_materialized_find(
        &mut self,
        py: Python<'_>,
        query: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let coll = self.parent_coll.bind(py).borrow();
        let docs = coll.find_via_engine(py, query)?;
        self.state = IterState::GeoNearOrdered { docs, idx: 0 };
        Ok(())
    }

    fn init_full_scan(&mut self) -> PyResult<()> {
        let session = self.borrow_session()?;
        let cursor = session.open_cursor(&self.table_uri, None).py()?;
        self.state = IterState::FullScan { cursor };
        Ok(())
    }

    fn advance_full_scan(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let cursor = match &mut self.state {
            IterState::FullScan { cursor } => cursor,
            _ => return Ok(None),
        };

        let query = self.query.bind(py);
        let empty = query.is_empty();

        loop {
            if cursor.next().is_err() {
                let old = std::mem::replace(&mut self.state, IterState::Exhausted);
                if let IterState::FullScan { mut cursor } = old {
                    cursor.close().py()?;
                }
                self.release_locks(py);
                return Ok(None);
            }
            let raw = cursor.get_value_raw().py()?;
            let doc = bson_helpers::from_bson(py, &raw)?;
            if empty || query_compiler::eval_query(&doc, query)? {
                return Ok(Some(doc.unbind()));
            }
        }
    }

    fn advance_id_scan(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let (cursor, ids, idx) = match &mut self.state {
            IterState::IdScan { cursor, ids, idx } => (cursor, ids, idx),
            _ => return Ok(None),
        };

        let query = self.query.bind(py);

        while *idx < ids.len() {
            let doc_id = &ids[*idx];
            *idx += 1;
            cursor.set_key_str(doc_id);
            if cursor.search().is_ok() {
                let raw = cursor.get_value_raw().py()?;
                let doc = bson_helpers::from_bson(py, &raw)?;
                if query.is_empty() || query_compiler::eval_query(&doc, query)? {
                    return Ok(Some(doc.unbind()));
                }
            }
        }

        let old = std::mem::replace(&mut self.state, IterState::Exhausted);
        if let IterState::IdScan { mut cursor, .. } = old {
            cursor.close().py()?;
        }
        self.release_locks(py);
        Ok(None)
    }

    fn advance_geo_near_ordered(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let (docs, idx) = match &mut self.state {
            IterState::GeoNearOrdered { docs, idx } => (docs, idx),
            _ => return Ok(None),
        };

        if *idx < docs.len() {
            let doc = docs[*idx].clone_ref(py);
            *idx += 1;
            return Ok(Some(doc));
        }

        let old = std::mem::replace(&mut self.state, IterState::Exhausted);
        if let IterState::GeoNearOrdered { .. } = old {
            self.release_locks(py);
        }
        Ok(None)
    }
}

#[pymethods]
impl RustStreamingCursor {
    #[new]
    #[pyo3(signature = (collection, query=None))]
    fn new(
        py: Python<'_>,
        collection: &Bound<'_, PyAny>,
        query: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let coll = collection
            .cast::<crate::local_collection::RustLocalCollection>()
            .map_err(|_| {
                PyRuntimeError::new_err("RustStreamingCursor requires RustLocalCollection")
            })?;
        let parent_coll = coll.clone().unbind();
        let coll_ref = coll.borrow();

        let (table_uri, rwlock, mutex, planner, session_py) = (
            coll_ref.table_uri.clone(),
            Arc::clone(&coll_ref.rwlock),
            Arc::clone(&coll_ref.mutex),
            coll_ref.planner.clone_ref(py),
            coll_ref.session_py.clone_ref(py).into_any(),
        );
        drop(coll_ref);

        let session_raw = {
            let sess_obj = session_py.bind(py);
            if let Ok(rs) = sess_obj.extract::<Py<crate::wt_bridge::RustWtSession>>() {
                let bound = rs.bind(py).borrow();
                bound.get().py()?.raw_ptr()
            } else {
                return Err(PyRuntimeError::new_err(
                    "StreamingCursor requires RustWtSession",
                ));
            }
        };

        let q = query
            .map(|q| q.unbind())
            .unwrap_or_else(|| PyDict::new(py).unbind());

        Ok(Self {
            parent_coll,
            table_uri,
            query: q,
            rwlock,
            mutex,
            planner,
            session_py,
            session_raw: Some(session_raw),
            state: IterState::NotStarted,
            mutex_guard: None,
        })
    }

    fn __iter__(mut slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.init_state(py)?;
        Ok(slf)
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        match &self.state {
            IterState::NotStarted => {
                self.init_state(py)?;
                self.__next__(py)
            }
            IterState::FullScan { .. } => self.advance_full_scan(py),
            IterState::IdScan { .. } => self.advance_id_scan(py),
            IterState::GeoNearOrdered { .. } => self.advance_geo_near_ordered(py),
            IterState::SingleDoc { .. } => {
                let old_state = std::mem::replace(&mut self.state, IterState::Exhausted);
                if let IterState::SingleDoc { doc } = old_state {
                    self.release_locks(py);
                    Ok(doc)
                } else {
                    Ok(None)
                }
            }
            IterState::Exhausted => Ok(None),
        }
    }
}

impl Drop for RustStreamingCursor {
    fn drop(&mut self) {
        let old = std::mem::replace(&mut self.state, IterState::Exhausted);
        match old {
            IterState::FullScan { mut cursor } => {
                let _ = cursor.close();
            }
            IterState::IdScan { mut cursor, .. } => {
                let _ = cursor.close();
            }
            _ => {}
        }
    }
}
