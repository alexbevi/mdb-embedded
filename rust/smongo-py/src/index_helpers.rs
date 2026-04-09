//! Text index helpers and DuplicateKeyError.
use md5::{Digest, Md5};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

pyo3::create_exception!(
    smongo._smongo_core,
    DuplicateKeyError,
    pyo3::exceptions::PyException
);

fn word_regex() -> Regex {
    Regex::new(r"\w+").unwrap_or_else(|e| unreachable!("WORD_RE compile failed: {e}"))
}

static WORD_RE: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(word_regex);

/// NFKD-normalise + regex-split into lowercase tokens (mirrors `_tokenize`).
#[pyfunction]
pub(crate) fn rs_tokenize(text: &str) -> Vec<String> {
    let normalized: String = text.nfkd().collect();
    WORD_RE
        .find_iter(&normalized)
        .map(|m| m.as_str().to_lowercase())
        .collect()
}

/// MD5-hash a Python value for hashed indexes.
#[pyfunction]
pub(crate) fn rs_hash_value(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<String> {
    let json_mod = crate::cached_modules::json_mod(py)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("sort_keys", true)?;
    let str_cls = py.eval(c"str", None, None)?;
    kwargs.set_item("default", str_cls)?;
    let serialized: String = json_mod
        .call_method("dumps", (value,), Some(&kwargs))?
        .extract()?;
    let mut hasher = Md5::new();
    hasher.update(serialized.as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

/// Recursively flatten a dict into `(dotted_path, value)` pairs.
#[pyfunction]
pub(crate) fn rs_flatten_doc<'py>(
    py: Python<'py>,
    doc: &Bound<'py, PyAny>,
) -> PyResult<Vec<(String, Py<PyAny>)>> {
    let mut out = Vec::new();
    if let Ok(d) = doc.cast::<PyDict>() {
        flatten_inner(py, d, "", &mut out)?;
    }
    Ok(out)
}

fn flatten_inner(
    _py: Python<'_>,
    doc: &Bound<'_, PyDict>,
    prefix: &str,
    out: &mut Vec<(String, Py<PyAny>)>,
) -> PyResult<()> {
    for (k, v) in doc.iter() {
        let key: String = k.extract().unwrap_or_default();
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        if let Ok(d) = v.cast::<PyDict>() {
            flatten_inner(_py, d, &path, out)?;
        } else if let Ok(list) = v.cast::<PyList>() {
            for (i, item) in list.iter().enumerate() {
                if let Ok(d) = item.cast::<PyDict>() {
                    flatten_inner(_py, d, &format!("{path}.{i}"), out)?;
                } else {
                    out.push((path.clone(), item.unbind()));
                }
            }
        } else {
            out.push((path, v.unbind()));
        }
    }
    Ok(())
}
