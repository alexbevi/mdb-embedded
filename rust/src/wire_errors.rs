//! Mongo-compatible error codes and response formatting for the wire layer.

use pyo3::prelude::*;
use pyo3::types::PyDict;

fn lookup_error(name: &str) -> (i32, &'static str) {
    match name {
        "InternalError" => (1, "InternalError"),
        "BadValue" => (2, "BadValue"),
        "NoSuchKey" => (4, "NoSuchKey"),
        "Unauthorized" => (13, "Unauthorized"),
        "TypeMismatch" => (14, "TypeMismatch"),
        "AuthenticationFailed" => (18, "AuthenticationFailed"),
        "IllegalOperation" => (20, "IllegalOperation"),
        "NamespaceNotFound" => (26, "NamespaceNotFound"),
        "IndexNotFound" => (27, "IndexNotFound"),
        "CursorNotFound" => (43, "CursorNotFound"),
        "NamespaceExists" => (48, "NamespaceExists"),
        "ExceededTimeLimit" => (50, "ExceededTimeLimit"),
        "CommandNotFound" => (59, "CommandNotFound"),
        "WriteConcernFailed" => (64, "WriteConcernFailed"),
        "InvalidLength" => (70, "InvalidLength"),
        "InvalidOptions" => (72, "InvalidOptions"),
        "InvalidNamespace" => (73, "InvalidNamespace"),
        "OperationFailed" => (96, "OperationFailed"),
        "CommandNotSupported" => (115, "CommandNotSupported"),
        "DocumentValidationFailure" => (121, "DocumentValidationFailure"),
        "DuplicateKey" => (11000, "DuplicateKey"),
        "NotPrimaryOrSecondary" => (13436, "NotPrimaryOrSecondary"),
        _ => (1, "InternalError"),
    }
}

#[pyfunction]
pub fn error_response<'py>(
    py: Python<'py>,
    code: i32,
    code_name: &str,
    message: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("ok", 0)?;
    dict.set_item("errmsg", message)?;
    dict.set_item("code", code)?;
    dict.set_item("codeName", code_name)?;
    Ok(dict)
}

#[pyfunction]
pub fn make_error<'py>(py: Python<'py>, name: &str, message: &str) -> PyResult<Bound<'py, PyDict>> {
    let (code, code_name) = lookup_error(name);
    error_response(py, code, code_name, message)
}
