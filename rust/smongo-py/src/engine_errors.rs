//! Map smongo-engine `CollectionError` to Python exceptions (wire / PyMongo parity).

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::index_helpers::DuplicateKeyError;
use smongo_engine::collection::CollectionError;
use smongo_engine::database::DatabaseError;

pub(crate) fn map_collection_error(err: CollectionError, op: &str) -> PyErr {
    match err {
        CollectionError::UniqueConstraintViolation(msg) => {
            DuplicateKeyError::new_err(format!("E11000 duplicate key error: {}", msg))
        }
        other => PyRuntimeError::new_err(format!("{}: {}", op, other)),
    }
}

pub(crate) fn map_database_error(err: DatabaseError, op: &str) -> PyErr {
    match err {
        DatabaseError::CollectionError(ce) => map_collection_error(ce, op),
        other => PyRuntimeError::new_err(format!("{}: {}", op, other)),
    }
}
