//! Shared parsing for `$near` / `$nearSphere` field conditions (planner + query compiler).

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// Parse `{ $near | $nearSphere, $maxDistance?, $minDistance? }` on an indexed field.
/// GeoJSON form nests `$geometry` / distances inside the `$near` document; legacy uses
/// top-level `$maxDistance` / `$minDistance` with `$near: [lon, lat]`.
/// (longitude, latitude, max_distance_m, min_distance_m)
pub(crate) type NearSpec = (f64, f64, Option<f64>, Option<f64>);

pub(crate) fn parse_field_near_spec(cond_dict: &Bound<'_, PyDict>) -> PyResult<Option<NearSpec>> {
    let outer_max = cond_dict
        .get_item("$maxDistance")?
        .and_then(|v| v.extract::<f64>().ok());
    let outer_min = cond_dict
        .get_item("$minDistance")?
        .and_then(|v| v.extract::<f64>().ok());

    let near_val = cond_dict
        .get_item("$near")?
        .or_else(|| cond_dict.get_item("$nearSphere").ok().flatten());

    let Some(nv) = near_val else {
        return Ok(None);
    };

    if let Ok(list) = nv.cast::<PyList>() {
        if list.len() < 2 {
            return Err(PyValueError::new_err(
                "legacy $near array must be [longitude, latitude]",
            ));
        }
        let lon: f64 = list.get_item(0)?.extract()?;
        let lat: f64 = list.get_item(1)?.extract()?;
        return Ok(Some((lon, lat, outer_max, outer_min)));
    }

    let spec = nv.cast::<PyDict>()?;
    let inner_max = spec
        .get_item("$maxDistance")?
        .and_then(|v| v.extract::<f64>().ok());
    let inner_min = spec
        .get_item("$minDistance")?
        .and_then(|v| v.extract::<f64>().ok());
    let max_d = inner_max.or(outer_max);
    let min_d = inner_min.or(outer_min);

    let geom = spec
        .get_item("$geometry")?
        .ok_or_else(|| PyValueError::new_err("$near requires $geometry or legacy [lon, lat]"))?;
    let g = geom.cast::<PyDict>()?;
    let typ: String = g
        .get_item("type")?
        .map(|v| v.extract())
        .transpose()?
        .unwrap_or_default();
    if typ != "Point" {
        return Err(PyValueError::new_err(
            "only GeoJSON Point is supported for $near",
        ));
    }
    let coords = g
        .get_item("coordinates")?
        .ok_or_else(|| PyValueError::new_err("Point.geometry requires coordinates"))?;
    let cl = coords.cast::<PyList>()?;
    if cl.len() < 2 {
        return Err(PyValueError::new_err("Point coordinates need [lon, lat]"));
    }
    let lon: f64 = cl.get_item(0)?.extract()?;
    let lat: f64 = cl.get_item(1)?.extract()?;
    Ok(Some((lon, lat, max_d, min_d)))
}

/// Inner value of `$geoWithin`: `{ $centerSphere: ... }` → center + radius (radians).
pub(crate) fn parse_geo_within_inner_center_sphere(
    inner: &Bound<'_, PyDict>,
) -> PyResult<Option<(f64, f64, f64)>> {
    let Some(cs) = inner.get_item("$centerSphere")? else {
        return Ok(None);
    };
    let list = cs.cast::<PyList>()?;
    if list.len() < 2 {
        return Err(PyValueError::new_err(
            "$centerSphere must be [ [longitude, latitude], radiusRadians ]",
        ));
    }
    let c0 = list.get_item(0)?;
    let center = c0.cast::<PyList>()?;
    if center.len() < 2 {
        return Err(PyValueError::new_err(
            "$centerSphere center must be [longitude, latitude]",
        ));
    }
    let lon: f64 = center.get_item(0)?.extract()?;
    let lat: f64 = center.get_item(1)?.extract()?;
    let radius_rad: f64 = list.get_item(1)?.extract()?;
    if radius_rad < 0.0 {
        return Err(PyValueError::new_err("$centerSphere radius must be >= 0"));
    }
    Ok(Some((lon, lat, radius_rad)))
}

/// Parse `{ $geoWithin: { $centerSphere: [ [lon, lat], radiusRadians ] } }` on an indexed field.
/// Returns `None` if `$geoWithin` or `$centerSphere` is absent (e.g. `$geometry` polygon).
pub(crate) fn parse_field_geo_within_center_sphere(
    cond_dict: &Bound<'_, PyDict>,
) -> PyResult<Option<(f64, f64, f64)>> {
    let Some(gw) = cond_dict.get_item("$geoWithin")? else {
        return Ok(None);
    };
    let inner = gw.cast::<PyDict>()?;
    parse_geo_within_inner_center_sphere(inner)
}

/// `$geoWithin` + `$geometry` Polygon/MultiPolygon (no `$centerSphere`).
pub(crate) fn parse_geo_within_inner_geometry(
    inner: &Bound<'_, PyDict>,
) -> PyResult<Option<crate::geo_polygon::GeoQueryShape>> {
    if parse_geo_within_inner_center_sphere(inner)?.is_some() {
        return Ok(None);
    }
    let Some(geom) = inner.get_item("$geometry")? else {
        return Ok(None);
    };
    let g = geom.cast::<PyDict>()?;
    Ok(Some(
        crate::geo_polygon::geo_query_shape_from_geometry_dict(g)?,
    ))
}

pub(crate) fn parse_field_geo_within_geometry(
    cond_dict: &Bound<'_, PyDict>,
) -> PyResult<Option<crate::geo_polygon::GeoQueryShape>> {
    let Some(gw) = cond_dict.get_item("$geoWithin")? else {
        return Ok(None);
    };
    let inner = gw.cast::<PyDict>()?;
    parse_geo_within_inner_geometry(inner)
}

/// `$geoIntersects` + `$geometry` Polygon/MultiPolygon.
pub(crate) fn parse_geo_intersects_inner_geometry(
    inner: &Bound<'_, PyDict>,
) -> PyResult<Option<crate::geo_polygon::GeoQueryShape>> {
    let Some(geom) = inner.get_item("$geometry")? else {
        return Ok(None);
    };
    let g = geom.cast::<PyDict>()?;
    Ok(Some(
        crate::geo_polygon::geo_query_shape_from_geometry_dict(g)?,
    ))
}

pub(crate) fn parse_field_geo_intersects_geometry(
    cond_dict: &Bound<'_, PyDict>,
) -> PyResult<Option<crate::geo_polygon::GeoQueryShape>> {
    let Some(gi) = cond_dict.get_item("$geoIntersects")? else {
        return Ok(None);
    };
    let inner = gi.cast::<PyDict>()?;
    parse_geo_intersects_inner_geometry(inner)
}
