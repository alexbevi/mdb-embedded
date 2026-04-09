//! GeoJSON Polygon / MultiPolygon → S2 covering + spherical point-in-polygon (great-circle edges).

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use s2::edgeutil::{distance_from_segment, simple_crossing};
use s2::latlng::LatLng;
use s2::point::Point;

/// On-edge tolerance (~1m on Earth).
const BOUNDARY_ANGLE_RAD: f64 = 1e-7;

/// Each polygon: `rings[0]` = exterior, `rings[1..]` = holes (reversed to CCW around hole for tests).
#[derive(Clone, Debug)]
pub struct GeoQueryShape {
    /// Per polygon: exterior + holes (hole vertices reversed so "inside hole" uses same CCW test).
    pub polygons: Vec<Vec<Vec<Point>>>,
}

impl GeoQueryShape {
    /// `$geoWithin`: in exterior and not in any hole (closed sets, boundary counts as inside / in hole).
    pub fn contains_point_lonlat(&self, lon_deg: f64, lat_deg: f64) -> bool {
        let ll = LatLng::from_degrees(lat_deg, lon_deg);
        let p = Point::from(ll.normalized());
        self.contains_point_s2(&p)
    }

    pub fn contains_point_s2(&self, p: &Point) -> bool {
        for poly in &self.polygons {
            if polygon_covers_point(poly, p) {
                return true;
            }
        }
        false
    }

    /// For a Point document, intersects matches contains (0-dimensional intersection).
    pub fn intersects_point_lonlat(&self, lon_deg: f64, lat_deg: f64) -> bool {
        self.contains_point_lonlat(lon_deg, lat_deg)
    }
}

fn polygon_covers_point(poly: &[Vec<Point>], p: &Point) -> bool {
    let Some(exterior) = poly.first() else {
        return false;
    };
    if !ring_contains_ccw(exterior, p) {
        return false;
    }
    for hole in poly.iter().skip(1) {
        if ring_contains_ccw(hole, p) {
            return false;
        }
    }
    true
}

/// Point-in-polygon on the sphere: CCW exterior (GeoJSON), great-circle edges, boundary included.
fn ring_contains_ccw(verts: &[Point], p: &Point) -> bool {
    if verts.len() < 3 {
        return false;
    }
    let n = verts.len();
    let m = if verts[0].approx_eq(&verts[n - 1]) {
        n - 1
    } else {
        n
    };
    if m < 3 {
        return false;
    }
    for i in 0..m {
        let a = verts[i];
        let b = verts[(i + 1) % m];
        if distance_from_segment(p, &a, &b).rad() <= BOUNDARY_ANGLE_RAD {
            return true;
        }
    }
    let ref_pt = Point::origin();
    if p.approx_eq(&ref_pt) {
        return false;
    }
    let mut inside = false;
    for i in 0..m {
        let a = verts[i];
        let b = verts[(i + 1) % m];
        if simple_crossing(p, &ref_pt, &a, &b) {
            inside = !inside;
        }
    }
    inside
}

/// Parse `{ type: "Polygon" | "MultiPolygon", coordinates: ... }` from a `$geometry` dict.
pub fn geo_query_shape_from_geometry_dict(g: &Bound<'_, PyDict>) -> PyResult<GeoQueryShape> {
    let typ: String = g
        .get_item("type")?
        .map(|v| v.extract::<String>())
        .transpose()?
        .unwrap_or_default();
    let coords_any = g
        .get_item("coordinates")?
        .ok_or_else(|| PyValueError::new_err("$geometry requires coordinates"))?;
    match typ.as_str() {
        "Polygon" => {
            let poly = parse_polygon_coordinates(&coords_any)?;
            Ok(GeoQueryShape {
                polygons: vec![poly],
            })
        }
        "MultiPolygon" => {
            let list = coords_any.cast::<PyList>()?;
            let mut polygons = Vec::new();
            for item in list.iter() {
                polygons.push(parse_polygon_coordinates(&item)?);
            }
            if polygons.is_empty() {
                return Err(PyValueError::new_err(
                    "MultiPolygon coordinates must be non-empty",
                ));
            }
            Ok(GeoQueryShape { polygons })
        }
        _ => Err(PyValueError::new_err(format!(
            "unsupported $geometry type for 2dsphere query: {typ} (expected Polygon or MultiPolygon)"
        ))),
    }
}

fn parse_polygon_coordinates(coords_any: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<Point>>> {
    let rings_py = coords_any.cast::<PyList>()?;
    if rings_py.is_empty() {
        return Err(PyValueError::new_err(
            "Polygon must have at least one linear ring",
        ));
    }
    let mut rings = Vec::new();
    for (ri, ring_any) in rings_py.iter().enumerate() {
        let ring = parse_linear_ring(&ring_any)?;
        if ri > 0 {
            let mut rev = ring.clone();
            rev.reverse();
            rings.push(rev);
        } else {
            rings.push(ring);
        }
    }
    Ok(rings)
}

fn parse_linear_ring(ring_any: &Bound<'_, PyAny>) -> PyResult<Vec<Point>> {
    let list = ring_any.cast::<PyList>()?;
    if list.len() < 4 {
        return Err(PyValueError::new_err(
            "each linear ring must have at least 4 positions (closed)",
        ));
    }
    let mut pts = Vec::with_capacity(list.len());
    for item in list.iter() {
        let pos = item.cast::<PyList>()?;
        if pos.len() < 2 {
            return Err(PyValueError::new_err(
                "ring position must be [longitude, latitude]",
            ));
        }
        let lon: f64 = pos.get_item(0)?.extract()?;
        let lat: f64 = pos.get_item(1)?.extract()?;
        if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
            return Err(PyValueError::new_err(format!(
                "invalid lon/lat: ({lon}, {lat})"
            )));
        }
        let ll = LatLng::from_degrees(lat, lon);
        pts.push(Point::from(ll.normalized()));
    }
    if !pts[0].approx_eq(&pts[pts.len() - 1]) {
        return Err(PyValueError::new_err(
            "linear ring must be closed (first position equals last)",
        ));
    }
    Ok(pts)
}
