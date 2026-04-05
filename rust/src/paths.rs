//! Dot-notation path traversal, field existence, set, and unset for nested documents.
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// Traverse a document using a dot-notation path.
///
/// Returns `None` when any segment is missing, the value is `None`,
/// or the intermediate type is neither `dict` nor `list`.
#[pyfunction]
pub fn get_value<'py>(doc: &Bound<'py, PyAny>, key: &str) -> PyResult<Py<PyAny>> {
    let py = doc.py();
    let mut val = doc.clone();

    for part in key.split('.') {
        if val.is_none() {
            return Ok(py.None());
        }
        if let Ok(d) = val.cast::<PyDict>() {
            match d.get_item(part)? {
                Some(v) => val = v,
                None => return Ok(py.None()),
            }
        } else if let Ok(l) = val.cast::<PyList>() {
            match part.parse::<usize>() {
                Ok(idx) if idx < l.len() => val = l.get_item(idx)?,
                _ => return Ok(py.None()),
            }
        } else {
            return Ok(py.None());
        }
    }

    Ok(val.unbind())
}

/// Check whether a dot-notation path exists in a document.
///
/// Unlike `get_value`, this distinguishes "field present with value None/null"
/// from "field missing entirely", matching MongoDB `$exists` semantics.
#[pyfunction]
pub fn field_exists(doc: &Bound<'_, PyAny>, key: &str) -> PyResult<bool> {
    let mut val = doc.clone();

    for part in key.split('.') {
        if let Ok(d) = val.cast::<PyDict>() {
            if !d.contains(part)? {
                return Ok(false);
            }
            val = {
                #[allow(clippy::expect_used)]
                d.get_item(part)?.expect("key confirmed present")
            };
        } else if let Ok(l) = val.cast::<PyList>() {
            match part.parse::<usize>() {
                Ok(idx) if idx < l.len() => val = l.get_item(idx)?,
                _ => return Ok(false),
            }
        } else {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Set a value in a document using dot-notation, creating intermediate dicts.
#[pyfunction]
pub fn set_value(doc: &Bound<'_, PyAny>, key: &str, value: Py<PyAny>) -> PyResult<()> {
    let py = doc.py();
    let parts: Vec<&str> = key.split('.').collect();
    let mut d = doc.clone();

    for &p in &parts[..parts.len() - 1] {
        let current = d.cast::<PyDict>().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err("intermediate value is not a dict")
        })?;
        let need_create = match current.get_item(p)? {
            Some(child) => child.cast::<PyDict>().is_err(),
            None => true,
        };
        if need_create {
            let new_dict = PyDict::new(py);
            current.set_item(p, &new_dict)?;
            d = new_dict.into_any();
        } else {
            d = {
                #[allow(clippy::expect_used)]
                current.get_item(p)?.expect("key confirmed present")
            };
        }
    }

    let final_dict = d
        .cast::<PyDict>()
        .map_err(|_| pyo3::exceptions::PyTypeError::new_err("intermediate value is not a dict"))?;
    final_dict.set_item(parts[parts.len() - 1], value)?;
    Ok(())
}

/// Remove a field from a document using dot-notation.
#[pyfunction]
pub fn unset_value(doc: &Bound<'_, PyAny>, key: &str) -> PyResult<()> {
    let parts: Vec<&str> = key.split('.').collect();
    let mut d = doc.clone();

    for &p in &parts[..parts.len() - 1] {
        if let Ok(dict) = d.cast::<PyDict>() {
            if !dict.contains(p)? {
                return Ok(());
            }
            let child = {
                #[allow(clippy::expect_used)]
                dict.get_item(p)?.expect("key confirmed present")
            };
            if child.cast::<PyDict>().is_err() {
                return Ok(());
            }
            d = child;
        } else {
            return Ok(());
        }
    }

    if let Ok(dict) = d.cast::<PyDict>() {
        let last = parts[parts.len() - 1];
        if dict.contains(last)? {
            dict.del_item(last)?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use std::sync::Once;

    static INIT: Once = Once::new();

    fn with_py<F>(f: F)
    where
        F: for<'py> FnOnce(Python<'py>),
    {
        INIT.call_once(|| {
            Python::initialize();
        });
        Python::attach(f);
    }

    #[test]
    fn test_get_value_simple() {
        with_py(|py| {
            let d = PyDict::new(py);
            d.set_item("a", 1).unwrap();
            let result = get_value(d.as_any(), "a").unwrap();
            assert_eq!(result.extract::<i64>(py).unwrap(), 1);
        });
    }

    #[test]
    fn test_get_value_nested() {
        with_py(|py| {
            let inner = PyDict::new(py);
            inner.set_item("b", 42).unwrap();
            let outer = PyDict::new(py);
            outer.set_item("a", inner).unwrap();
            let result = get_value(outer.as_any(), "a.b").unwrap();
            assert_eq!(result.extract::<i64>(py).unwrap(), 42);
        });
    }

    #[test]
    fn test_get_value_missing() {
        with_py(|py| {
            let d = PyDict::new(py);
            let result = get_value(d.as_any(), "nope").unwrap();
            assert!(result.is_none(py));
        });
    }

    #[test]
    fn test_get_value_list_index() {
        with_py(|py| {
            let list = PyList::new(py, [10, 20, 30]).unwrap();
            let d = PyDict::new(py);
            d.set_item("arr", list).unwrap();
            let result = get_value(d.as_any(), "arr.1").unwrap();
            assert_eq!(result.extract::<i64>(py).unwrap(), 20);
        });
    }

    #[test]
    fn test_field_exists_true() {
        with_py(|py| {
            let d = PyDict::new(py);
            d.set_item("a", py.None()).unwrap();
            assert!(field_exists(d.as_any(), "a").unwrap());
        });
    }

    #[test]
    fn test_field_exists_false() {
        with_py(|py| {
            let d = PyDict::new(py);
            assert!(!field_exists(d.as_any(), "missing").unwrap());
        });
    }

    #[test]
    fn test_set_value_simple() {
        with_py(|py| {
            let d = PyDict::new(py);
            let val: Py<PyAny> = 99i64.into_pyobject(py).unwrap().into_any().unbind();
            set_value(d.as_any(), "x", val).unwrap();
            let v = d.get_item("x").unwrap().unwrap();
            assert_eq!(v.extract::<i64>().unwrap(), 99);
        });
    }

    #[test]
    fn test_set_value_nested_creates_intermediates() {
        with_py(|py| {
            let d = PyDict::new(py);
            let val: Py<PyAny> = 5i64.into_pyobject(py).unwrap().into_any().unbind();
            set_value(d.as_any(), "a.b.c", val).unwrap();
            let result = get_value(d.as_any(), "a.b.c").unwrap();
            assert_eq!(result.extract::<i64>(py).unwrap(), 5);
        });
    }

    #[test]
    fn test_unset_value() {
        with_py(|py| {
            let d = PyDict::new(py);
            d.set_item("a", 1).unwrap();
            d.set_item("b", 2).unwrap();
            unset_value(d.as_any(), "a").unwrap();
            assert!(!d.contains("a").unwrap());
            assert!(d.contains("b").unwrap());
        });
    }

    #[test]
    fn test_unset_value_nested() {
        with_py(|py| {
            let inner = PyDict::new(py);
            inner.set_item("x", 1).unwrap();
            inner.set_item("y", 2).unwrap();
            let outer = PyDict::new(py);
            outer.set_item("a", inner).unwrap();
            unset_value(outer.as_any(), "a.x").unwrap();
            assert!(!field_exists(outer.as_any(), "a.x").unwrap());
            assert!(field_exists(outer.as_any(), "a.y").unwrap());
        });
    }

    #[test]
    fn test_unset_value_missing_is_noop() {
        with_py(|py| {
            let d = PyDict::new(py);
            d.set_item("a", 1).unwrap();
            unset_value(d.as_any(), "b.c.d").unwrap();
            assert!(d.contains("a").unwrap());
        });
    }
}
