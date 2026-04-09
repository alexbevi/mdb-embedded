//! Rust-native `$jsonSchema` document validation.
//!
//! Delegates to `smongo_engine::schema::validate_document` via BSON conversion.

use pyo3::prelude::*;
use pyo3::types::PyDict;

pyo3::create_exception!(
    smongo._smongo_core,
    ValidationError,
    pyo3::exceptions::PyException
);

#[pyfunction]
#[pyo3(name = "validate_document")]
pub fn py_validate_document(doc: &Bound<'_, PyDict>, schema: &Bound<'_, PyAny>) -> PyResult<()> {
    if schema.is_none() || !schema.is_truthy()? {
        return Ok(());
    }
    let schema_dict = schema
        .cast::<PyDict>()
        .map_err(|_| ValidationError::new_err("schema must be a dict"))?;
    if schema_dict.is_empty() {
        return Ok(());
    }
    let bson_doc = crate::bson_helpers::pydict_to_doc(doc)?;
    let bson_schema = crate::bson_helpers::pydict_to_doc(schema_dict)?;
    smongo_engine::schema::validate_document(&bson_doc, &bson_schema)
        .map_err(|e| ValidationError::new_err(e.to_string()))
}
