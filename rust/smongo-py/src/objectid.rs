//! MongoDB-compatible ObjectId generation and manipulation.
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use pyo3::class::basic::CompareOp;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyNotImplemented, PyString};

struct Globals {
    random_bytes: [u8; 5],
    counter: AtomicU32,
}

static GLOBALS: OnceLock<Globals> = OnceLock::new();

#[allow(clippy::expect_used)]
fn init_globals() -> Globals {
    let mut random = [0u8; 5];
    // Intentional panic: ObjectId cannot be initialized without OS randomness.
    getrandom::getrandom(&mut random).expect("failed to generate random bytes");
    let mut seed = [0u8; 3];
    // Intentional panic: same as above — counter seed requires OS randomness.
    getrandom::getrandom(&mut seed).expect("failed to seed counter");
    let counter_val = ((seed[0] as u32) << 16) | ((seed[1] as u32) << 8) | (seed[2] as u32);
    Globals {
        random_bytes: random,
        counter: AtomicU32::new(counter_val),
    }
}

fn globals() -> &'static Globals {
    GLOBALS.get_or_init(init_globals)
}

#[allow(clippy::expect_used)]
fn generate_raw() -> [u8; 12] {
    let g = globals();
    // Intentional panic: a system clock before UNIX epoch breaks ObjectId timestamps.
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs() as u32;
    let counter = g.counter.fetch_add(1, Ordering::SeqCst).wrapping_add(1) & 0x00FF_FFFF;
    let mut out = [0u8; 12];
    out[0..4].copy_from_slice(&ts.to_be_bytes());
    out[4..9].copy_from_slice(&g.random_bytes);
    out[9] = (counter >> 16) as u8;
    out[10] = (counter >> 8) as u8;
    out[11] = counter as u8;
    out
}

/// MongoDB-compatible ObjectId exposed to Python via PyO3.
///
/// Stores both a raw `[u8; 12]` for fast Rust-side access and a
/// `Py<PyBytes>` so that `obj._bytes` returns the same Python object
/// each time (preserving identity semantics from the pure-Python impl).
#[pyclass(module = "smongo._smongo_core")]
pub struct ObjectId {
    raw: [u8; 12],
    #[pyo3(get)]
    _bytes: Py<PyBytes>,
}

impl ObjectId {
    #[cfg(test)]
    pub(crate) fn generate(py: Python<'_>) -> Self {
        let raw = generate_raw();
        Self {
            raw,
            _bytes: pyo3::types::PyBytes::new(py, &raw).unbind(),
        }
    }

    pub(crate) fn hex(&self) -> String {
        hex::encode(self.raw)
    }

    pub fn raw_bytes(&self) -> [u8; 12] {
        self.raw
    }

    /// Construct directly from raw 12-byte array, skipping hex encode/decode.
    pub(crate) fn from_raw(py: Python<'_>, raw: [u8; 12]) -> Self {
        Self {
            raw,
            _bytes: pyo3::types::PyBytes::new(py, &raw).unbind(),
        }
    }

    /// Construct from a 24-character hex string. Crate-internal convenience.
    #[allow(clippy::expect_used)]
    pub(crate) fn from_hex(py: Python<'_>, s: &str) -> Result<Self, pyo3::PyErr> {
        if s.len() != 24 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Invalid ObjectId: '{s}'"
            )));
        }
        let decoded = hex::decode(s).map_err(|_| {
            pyo3::exceptions::PyValueError::new_err(format!("Invalid ObjectId: '{s}'"))
        })?;
        let raw: [u8; 12] = decoded
            .try_into()
            .expect("hex decoded 24-char string is always 12 bytes");
        Ok(Self {
            raw,
            _bytes: pyo3::types::PyBytes::new(py, &raw).unbind(),
        })
    }
}

#[pymethods]
impl ObjectId {
    #[new]
    #[pyo3(signature = (oid=None))]
    #[allow(clippy::expect_used)]
    fn new(py: Python<'_>, oid: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        match oid {
            None => {
                let raw = generate_raw();
                Ok(Self {
                    raw,
                    _bytes: PyBytes::new(py, &raw).unbind(),
                })
            }
            Some(obj) => {
                // ObjectId(existing_oid) -- share the underlying PyBytes
                if let Ok(other) = obj.extract::<PyRef<'_, ObjectId>>() {
                    return Ok(Self {
                        raw: other.raw,
                        _bytes: other._bytes.clone_ref(py),
                    });
                }
                // ObjectId("507f1f77bcf86cd799439011")
                if let Ok(s) = obj.cast::<PyString>() {
                    let s = s.to_str()?;
                    if s.len() == 24 {
                        if let Ok(decoded) = hex::decode(s) {
                            let raw: [u8; 12] = decoded
                                .try_into()
                                .expect("hex decoded 24-char string is always 12 bytes");
                            return Ok(Self {
                                raw,
                                _bytes: PyBytes::new(py, &raw).unbind(),
                            });
                        }
                    }
                    return Err(PyValueError::new_err(format!("Invalid ObjectId: '{s}'")));
                }
                // ObjectId(b"\x50\x7f...")
                if let Ok(b) = obj.cast::<PyBytes>() {
                    let slice = b.as_bytes();
                    if slice.len() == 12 {
                        let mut raw = [0u8; 12];
                        raw.copy_from_slice(slice);
                        return Ok(Self {
                            raw,
                            _bytes: b.clone().unbind(),
                        });
                    }
                }
                Err(PyValueError::new_err(format!(
                    "Invalid ObjectId: {}",
                    obj.repr()?
                )))
            }
        }
    }

    #[getter]
    fn generation_time<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ts = u32::from_be_bytes([self.raw[0], self.raw[1], self.raw[2], self.raw[3]]);
        let datetime = crate::cached_modules::datetime(py)?;
        let utc = datetime.getattr("timezone")?.getattr("utc")?;
        datetime
            .getattr("datetime")?
            .call_method1("fromtimestamp", (ts as f64, utc))
    }

    #[getter]
    fn binary(&self, py: Python<'_>) -> Py<PyBytes> {
        self._bytes.clone_ref(py)
    }

    fn __str__(&self) -> String {
        self.hex()
    }

    fn __repr__(&self) -> String {
        format!("ObjectId('{}')", self.hex())
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.raw.hash(&mut hasher);
        hasher.finish()
    }

    fn __richcmp__(&self, other: &Bound<'_, PyAny>, op: CompareOp) -> PyResult<Py<PyAny>> {
        let py = other.py();

        if let Ok(other_oid) = other.extract::<PyRef<'_, ObjectId>>() {
            let result = match op {
                CompareOp::Lt => self.raw < other_oid.raw,
                CompareOp::Le => self.raw <= other_oid.raw,
                CompareOp::Eq => self.raw == other_oid.raw,
                CompareOp::Ne => self.raw != other_oid.raw,
                CompareOp::Gt => self.raw > other_oid.raw,
                CompareOp::Ge => self.raw >= other_oid.raw,
            };
            return Ok(result.into_pyobject(py)?.to_owned().into_any().unbind());
        }

        if matches!(op, CompareOp::Eq | CompareOp::Ne) {
            if let Ok(s) = other.extract::<&str>() {
                let eq = self.hex() == s;
                let result = if matches!(op, CompareOp::Eq) { eq } else { !eq };
                return Ok(result.into_pyobject(py)?.to_owned().into_any().unbind());
            }
        }

        Ok(PyNotImplemented::get(py).to_owned().into_any().unbind())
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, (String,))> {
        let cls = py.get_type::<ObjectId>().unbind().into_any();
        Ok((cls, (self.hex(),)))
    }

    fn __deepcopy__(&self, py: Python<'_>, _memo: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            raw: self.raw,
            _bytes: PyBytes::new(py, &self.raw).unbind(),
        })
    }

    fn __copy__(&self, py: Python<'_>) -> PyResult<Self> {
        Ok(Self {
            raw: self.raw,
            _bytes: PyBytes::new(py, &self.raw).unbind(),
        })
    }

    #[staticmethod]
    fn is_valid(oid: &Bound<'_, PyAny>) -> bool {
        if oid.is_instance_of::<ObjectId>() {
            return true;
        }
        if let Ok(s) = oid.cast::<PyString>() {
            if let Ok(s) = s.to_str() {
                return s.len() == 24 && hex::decode(s).is_ok();
            }
        }
        false
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn generate_raw_produces_12_bytes() {
        assert_eq!(generate_raw().len(), 12);
    }

    #[test]
    fn generate_raw_is_unique() {
        assert_ne!(generate_raw(), generate_raw());
    }

    #[test]
    fn generate_raw_embeds_current_timestamp() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        let raw = generate_raw();
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        let ts = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
        assert!(ts >= before && ts <= after);
    }

    #[test]
    fn hex_roundtrip() {
        let raw = generate_raw();
        let encoded = hex::encode(raw);
        assert_eq!(encoded.len(), 24);
        let decoded = hex::decode(&encoded).unwrap();
        assert_eq!(&decoded[..], &raw[..]);
    }

    #[test]
    fn counter_increments() {
        // Grab the counter value immediately before and after to avoid
        // interference from other parallel tests calling generate_raw().
        let g = globals();
        let before = g.counter.load(Ordering::SeqCst) & 0x00FF_FFFF;
        let raw = generate_raw();
        let in_oid = u32::from_be_bytes([0, raw[9], raw[10], raw[11]]);
        // The OID's counter must be strictly ahead of the snapshot we took
        // (it was fetched-and-incremented after our load).
        let delta = in_oid.wrapping_sub(before) & 0x00FF_FFFF;
        assert!(
            delta >= 1 && delta <= 64,
            "counter delta {delta} out of expected range"
        );
    }
}
