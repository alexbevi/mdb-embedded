//! Lightweight result types for insert, update, and delete operations.
use pyo3::prelude::*;

/// Result of an insert_one or insert_many operation.
#[pyclass(module = "smongo._smongo_core")]
pub struct InsertResult {
    #[pyo3(get)]
    pub inserted_ids: Py<PyAny>,
}

impl InsertResult {
    pub(crate) fn new(inserted_ids: Py<PyAny>) -> Self {
        Self { inserted_ids }
    }
}

#[pymethods]
impl InsertResult {
    #[new]
    fn py_new(inserted_ids: Py<PyAny>) -> Self {
        Self::new(inserted_ids)
    }
}

/// Result of an update_one, update_many, or replace operation.
#[pyclass(module = "smongo._smongo_core")]
pub struct UpdateResult {
    #[pyo3(get)]
    pub matched_count: i64,
    #[pyo3(get)]
    pub modified_count: i64,
    #[pyo3(get)]
    pub upserted_id: Py<PyAny>,
}

impl UpdateResult {
    pub(crate) fn new(
        py: Python<'_>,
        matched_count: i64,
        modified_count: i64,
        upserted_id: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            matched_count,
            modified_count,
            upserted_id: upserted_id.unwrap_or_else(|| py.None()),
        }
    }
}

#[pymethods]
impl UpdateResult {
    #[new]
    #[pyo3(signature = (matched_count, modified_count, upserted_id=None))]
    fn py_new(
        py: Python<'_>,
        matched_count: i64,
        modified_count: i64,
        upserted_id: Option<Py<PyAny>>,
    ) -> Self {
        Self::new(py, matched_count, modified_count, upserted_id)
    }
}

/// Result of a delete_one or delete_many operation.
#[pyclass(module = "smongo._smongo_core")]
pub struct DeleteResult {
    #[pyo3(get)]
    pub deleted_count: i64,
}

impl DeleteResult {
    pub(crate) fn new(deleted_count: i64) -> Self {
        Self { deleted_count }
    }
}

#[pymethods]
impl DeleteResult {
    #[new]
    fn py_new(deleted_count: i64) -> Self {
        Self::new(deleted_count)
    }
}
