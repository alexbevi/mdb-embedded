//! Great-circle helpers for Python-side query evaluation (`query_compiler`).
//! Indexed `2dsphere` execution lives in `smongo-engine`.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// Earth radius in meters (WGS84-ish; matches `smongo/aggregation/geo.py`).
pub const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

/// When `$maxDistance` is omitted, cap index probes and filtering at this radius (meters)
/// so S2 covering stays bounded (~20,000 km).
pub const DEFAULT_NEAR_MAX_DISTANCE_M: f64 = 20_000_000.0;

/// Great-circle distance in meters between two WGS84 (lon, lat) degrees.
pub fn haversine_meters(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1_r.cos() * lat2_r.cos() * (dlon / 2.0).sin().powi(2);
    EARTH_RADIUS_METERS * 2.0 * a.sqrt().asin()
}

/// GeoJSON Point or legacy `[lon, lat]` → `(lon, lat)` degrees.
pub fn extract_lon_lat_py(val: &Bound<'_, PyAny>) -> PyResult<Option<(f64, f64)>> {
    if val.is_none() {
        return Ok(None);
    }
    if let Ok(dict) = val.cast::<PyDict>() {
        let typ: String = dict
            .get_item("type")?
            .map(|v| v.extract::<String>())
            .transpose()?
            .unwrap_or_default();
        if typ == "Point" {
            let coords = dict.get_item("coordinates")?.ok_or_else(|| {
                PyValueError::new_err("GeoJSON Point requires 'coordinates'")
            })?;
            let list = coords.cast::<PyList>()?;
            if list.len() < 2 {
                return Err(PyValueError::new_err(
                    "Point.coordinates must have at least [longitude, latitude]",
                ));
            }
            let lon: f64 = list.get_item(0)?.extract()?;
            let lat: f64 = list.get_item(1)?.extract()?;
            return Ok(Some((lon, lat)));
        }
        return Ok(None);
    }
    if let Ok(list) = val.cast::<PyList>() {
        if list.len() >= 2 {
            let lon: f64 = list.get_item(0)?.extract()?;
            let lat: f64 = list.get_item(1)?.extract()?;
            return Ok(Some((lon, lat)));
        }
    }
    Ok(None)
}
