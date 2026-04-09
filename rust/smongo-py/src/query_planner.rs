//! Rust port of `smongo.index.QueryPlanner`.
//!
//! Chooses the best execution strategy for a given query and executes
//! index scans directly via WtCursor -- no Python dispatch on the hot path.

use std::collections::{HashMap, HashSet};
use std::mem::ManuallyDrop;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};

use crate::geo_query::{
    collect_field_conditions, parse_field_geo_intersects_geometry,
    parse_field_geo_within_center_sphere, parse_field_geo_within_geometry, parse_field_near_spec,
};
use crate::geo_s2;
use s2::cellid::CellID;
use crate::index_encoding::{invert_encoded_impl, sortable_encode_impl};
use crate::wt_bridge::WtResultExt;
use crate::index_manager::{IndexDef, IndexDir, IndexType, RustIndexManager};
use crate::wt_safe::WtSession;

// ---------------------------------------------------------------------------
// QueryPlan
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PlanType {
    CollectionScan,
    PkLookup,
    IndexScan,
    GeoNear,
    GeoWithin,
    GeoIntersects,
    OrUnion,
}

impl PlanType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::CollectionScan => "collection_scan",
            Self::PkLookup => "pk_lookup",
            Self::IndexScan => "index_scan",
            Self::GeoNear => "geo_near",
            Self::GeoWithin => "geo_within",
            Self::GeoIntersects => "geo_intersects",
            Self::OrUnion => "or_union",
        }
    }
}

#[derive(Clone)]
pub(crate) struct BoundSegment {
    pub encoded: Option<String>,
    pub inclusive: bool,
}

pub(crate) type BoundsResult = (i32, (Vec<BoundSegment>, Vec<BoundSegment>));

pub(crate) struct RejectedPlan {
    pub index_name: String,
    pub score: i32,
}

pub(crate) struct QueryPlan {
    pub plan_type: PlanType,
    pub index_name: Option<String>,
    pub bounds: Option<(Vec<BoundSegment>, Vec<BoundSegment>)>,
    pub index_def_name: Option<String>,
    pub subplans: Option<Vec<QueryPlan>>,
    pub rejected_plans: Vec<RejectedPlan>,
    /// Set when `plan_type` is `GeoNear` or `GeoWithin` (cap / distance filter).
    pub geo_field: Option<String>,
    pub geo_lon: Option<f64>,
    pub geo_lat: Option<f64>,
    pub geo_max_distance_m: Option<f64>,
    pub geo_min_distance_m: Option<f64>,
    /// `$geoWithin` / `$geoIntersects` with `$geometry`: precomputed S2 cover (conservative superset).
    pub geo_cover_cells: Option<Vec<CellID>>,
    /// Parsed Polygon/MultiPolygon for post-filter in `eval_query`.
    pub geo_shape: Option<crate::geo_polygon::GeoQueryShape>,
    /// `"Polygon"` or `"MultiPolygon"` for explain.
    pub geo_geometry_type: Option<String>,
}

impl QueryPlan {
    fn collection_scan() -> Self {
        Self {
            plan_type: PlanType::CollectionScan,
            index_name: None,
            bounds: None,
            index_def_name: None,
            subplans: None,
            rejected_plans: Vec::new(),
            geo_field: None,
            geo_lon: None,
            geo_lat: None,
            geo_max_distance_m: None,
            geo_min_distance_m: None,
            geo_cover_cells: None,
            geo_shape: None,
            geo_geometry_type: None,
        }
    }

    fn pk_lookup() -> Self {
        Self {
            plan_type: PlanType::PkLookup,
            index_name: None,
            bounds: None,
            index_def_name: None,
            subplans: None,
            rejected_plans: Vec::new(),
            geo_field: None,
            geo_lon: None,
            geo_lat: None,
            geo_max_distance_m: None,
            geo_min_distance_m: None,
            geo_cover_cells: None,
            geo_shape: None,
            geo_geometry_type: None,
        }
    }

    fn index_scan(name: String, bounds: (Vec<BoundSegment>, Vec<BoundSegment>)) -> Self {
        Self {
            plan_type: PlanType::IndexScan,
            index_name: Some(name.clone()),
            bounds: Some(bounds),
            index_def_name: Some(name),
            subplans: None,
            rejected_plans: Vec::new(),
            geo_field: None,
            geo_lon: None,
            geo_lat: None,
            geo_max_distance_m: None,
            geo_min_distance_m: None,
            geo_cover_cells: None,
            geo_shape: None,
            geo_geometry_type: None,
        }
    }

    fn geo_near(
        name: String,
        field: String,
        lon: f64,
        lat: f64,
        max_m: Option<f64>,
        min_m: Option<f64>,
    ) -> Self {
        Self {
            plan_type: PlanType::GeoNear,
            index_name: Some(name.clone()),
            bounds: None,
            index_def_name: Some(name),
            subplans: None,
            rejected_plans: Vec::new(),
            geo_field: Some(field),
            geo_lon: Some(lon),
            geo_lat: Some(lat),
            geo_max_distance_m: max_m,
            geo_min_distance_m: min_m,
            geo_cover_cells: None,
            geo_shape: None,
            geo_geometry_type: None,
        }
    }

    /// `$geoWithin` + `$centerSphere`: `max_distance_m` is `radiusRadians * earthRadius`.
    fn geo_within(name: String, field: String, lon: f64, lat: f64, max_distance_m: f64) -> Self {
        Self {
            plan_type: PlanType::GeoWithin,
            index_name: Some(name.clone()),
            bounds: None,
            index_def_name: Some(name),
            subplans: None,
            rejected_plans: Vec::new(),
            geo_field: Some(field),
            geo_lon: Some(lon),
            geo_lat: Some(lat),
            geo_max_distance_m: Some(max_distance_m),
            geo_min_distance_m: None,
            geo_cover_cells: None,
            geo_shape: None,
            geo_geometry_type: None,
        }
    }

    /// `$geoWithin` + `$geometry` Polygon/MultiPolygon.
    fn geo_within_geometry(
        name: String,
        field: String,
        shape: crate::geo_polygon::GeoQueryShape,
        geometry_type: String,
    ) -> Self {
        let cells = shape.covering_cell_ids();
        Self {
            plan_type: PlanType::GeoWithin,
            index_name: Some(name.clone()),
            bounds: None,
            index_def_name: Some(name),
            subplans: None,
            rejected_plans: Vec::new(),
            geo_field: Some(field),
            geo_lon: None,
            geo_lat: None,
            geo_max_distance_m: None,
            geo_min_distance_m: None,
            geo_cover_cells: Some(cells),
            geo_shape: Some(shape),
            geo_geometry_type: Some(geometry_type),
        }
    }

    /// `$geoIntersects` + `$geometry` Polygon/MultiPolygon (document Point only).
    fn geo_intersects_geometry(
        name: String,
        field: String,
        shape: crate::geo_polygon::GeoQueryShape,
        geometry_type: String,
    ) -> Self {
        let cells = shape.covering_cell_ids();
        Self {
            plan_type: PlanType::GeoIntersects,
            index_name: Some(name.clone()),
            bounds: None,
            index_def_name: Some(name),
            subplans: None,
            rejected_plans: Vec::new(),
            geo_field: Some(field),
            geo_lon: None,
            geo_lat: None,
            geo_max_distance_m: None,
            geo_min_distance_m: None,
            geo_cover_cells: Some(cells),
            geo_shape: Some(shape),
            geo_geometry_type: Some(geometry_type),
        }
    }

    fn or_union(subplans: Vec<QueryPlan>) -> Self {
        Self {
            plan_type: PlanType::OrUnion,
            index_name: None,
            bounds: None,
            index_def_name: None,
            subplans: Some(subplans),
            rejected_plans: Vec::new(),
            geo_field: None,
            geo_lon: None,
            geo_lat: None,
            geo_max_distance_m: None,
            geo_min_distance_m: None,
            geo_cover_cells: None,
            geo_shape: None,
            geo_geometry_type: None,
        }
    }

    pub(crate) fn to_py_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        d.set_item("plan", self.plan_type.as_str())?;
        if let Some(ref name) = self.index_name {
            d.set_item("index", name)?;
        }
        if let Some((ref lower, ref upper)) = self.bounds {
            let bounds = PyDict::new(py);
            let lower_list = PyList::empty(py);
            for seg in lower {
                if let Some(ref enc) = seg.encoded {
                    let label = if seg.inclusive {
                        "inclusive"
                    } else {
                        "exclusive"
                    };
                    let pair = pyo3::types::PyTuple::new(
                        py,
                        [
                            enc.into_pyobject(py)?.into_any(),
                            label.into_pyobject(py)?.into_any(),
                        ],
                    )?;
                    lower_list.append(pair)?;
                }
            }
            let upper_list = PyList::empty(py);
            for seg in upper {
                if let Some(ref enc) = seg.encoded {
                    let label = if seg.inclusive {
                        "inclusive"
                    } else {
                        "exclusive"
                    };
                    let pair = pyo3::types::PyTuple::new(
                        py,
                        [
                            enc.into_pyobject(py)?.into_any(),
                            label.into_pyobject(py)?.into_any(),
                        ],
                    )?;
                    upper_list.append(pair)?;
                }
            }
            bounds.set_item("lower", lower_list)?;
            bounds.set_item("upper", upper_list)?;
            d.set_item("indexBounds", bounds)?;
        }
        if let Some(ref subs) = self.subplans {
            let sub_list = PyList::empty(py);
            for sp in subs {
                sub_list.append(sp.to_py_dict(py)?)?;
            }
            d.set_item("subplans", sub_list)?;
        }
        if !self.rejected_plans.is_empty() {
            let rej_list = PyList::empty(py);
            for rp in &self.rejected_plans {
                let rd = PyDict::new(py);
                rd.set_item("index", &rp.index_name)?;
                rd.set_item("score", rp.score)?;
                rej_list.append(rd)?;
            }
            d.set_item("rejectedPlans", rej_list)?;
        }
        if matches!(
            self.plan_type,
            PlanType::GeoNear | PlanType::GeoWithin | PlanType::GeoIntersects
        ) {
            if let Some(ref f) = self.geo_field {
                d.set_item("geoField", f)?;
            }
            match self.plan_type {
                PlanType::GeoWithin if self.geo_shape.is_some() => {
                    d.set_item("geoPredicate", "within")?;
                    if let Some(ref gt) = self.geo_geometry_type {
                        d.set_item("geometryType", gt)?;
                    }
                    if let Some(ref cells) = self.geo_cover_cells {
                        d.set_item("coveringCellCount", cells.len())?;
                    }
                }
                PlanType::GeoIntersects => {
                    d.set_item("geoPredicate", "intersects")?;
                    if let Some(ref gt) = self.geo_geometry_type {
                        d.set_item("geometryType", gt)?;
                    }
                    if let Some(ref cells) = self.geo_cover_cells {
                        d.set_item("coveringCellCount", cells.len())?;
                    }
                }
                _ => {
                    if let (Some(lon), Some(lat)) = (self.geo_lon, self.geo_lat) {
                        let center = PyList::new(py, [lon, lat])?;
                        d.set_item("center", center)?;
                    }
                    if let Some(m) = self.geo_max_distance_m {
                        d.set_item("maxDistanceMeters", m)?;
                    }
                    if let Some(m) = self.geo_min_distance_m {
                        d.set_item("minDistanceMeters", m)?;
                    }
                    if self.plan_type == PlanType::GeoWithin {
                        if let Some(m) = self.geo_max_distance_m {
                            d.set_item(
                                "centerSphereRadiusRadians",
                                m / geo_s2::EARTH_RADIUS_METERS,
                            )?;
                        }
                    }
                }
            }
        }
        Ok(d.unbind())
    }
}

fn try_geo_near_plan(
    mgr: &RustIndexManager,
    field_conditions: &HashMap<String, Bound<'_, PyAny>>,
) -> PyResult<Option<QueryPlan>> {
    for (name, idx) in mgr.indexes() {
        if idx.index_type != IndexType::TwoDsphere {
            continue;
        }
        let field = match idx.keys.first() {
            Some((f, _)) => f.as_str(),
            None => continue,
        };
        let cond = match field_conditions.get(field) {
            Some(c) => c,
            None => continue,
        };
        let Ok(cond_dict) = cond.cast::<PyDict>() else {
            continue;
        };
        if let Some((lon, lat, max_m, min_m)) = parse_field_near_spec(&cond_dict)? {
            return Ok(Some(QueryPlan::geo_near(
                name.clone(),
                field.to_string(),
                lon,
                lat,
                Some(max_m.unwrap_or(geo_s2::DEFAULT_NEAR_MAX_DISTANCE_M)),
                min_m,
            )));
        }
    }
    Ok(None)
}

fn geometry_type_label(inner: &Bound<'_, PyDict>) -> PyResult<String> {
    let Some(geom) = inner.get_item("$geometry")? else {
        return Ok(String::new());
    };
    let Ok(g) = geom.cast::<PyDict>() else {
        return Ok(String::new());
    };
    Ok(g
        .get_item("type")?
        .and_then(|t| t.extract::<String>().ok())
        .unwrap_or_default())
}

fn try_geo_intersects_plan(
    mgr: &RustIndexManager,
    field_conditions: &HashMap<String, Bound<'_, PyAny>>,
) -> PyResult<Option<QueryPlan>> {
    for (name, idx) in mgr.indexes() {
        if idx.index_type != IndexType::TwoDsphere {
            continue;
        }
        let field = match idx.keys.first() {
            Some((f, _)) => f.as_str(),
            None => continue,
        };
        let cond = match field_conditions.get(field) {
            Some(c) => c,
            None => continue,
        };
        let Ok(cond_dict) = cond.cast::<PyDict>() else {
            continue;
        };
        if let Some(shape) = parse_field_geo_intersects_geometry(&cond_dict)? {
            let inner = cond_dict
                .get_item("$geoIntersects")?
                .ok_or_else(|| PyRuntimeError::new_err("$geoIntersects missing"))?;
            let inner_d = inner.cast::<PyDict>()?;
            let gt = geometry_type_label(&inner_d)?;
            return Ok(Some(QueryPlan::geo_intersects_geometry(
                name.clone(),
                field.to_string(),
                shape,
                gt,
            )));
        }
    }
    Ok(None)
}

fn try_geo_within_plan(
    mgr: &RustIndexManager,
    field_conditions: &HashMap<String, Bound<'_, PyAny>>,
) -> PyResult<Option<QueryPlan>> {
    for (name, idx) in mgr.indexes() {
        if idx.index_type != IndexType::TwoDsphere {
            continue;
        }
        let field = match idx.keys.first() {
            Some((f, _)) => f.as_str(),
            None => continue,
        };
        let cond = match field_conditions.get(field) {
            Some(c) => c,
            None => continue,
        };
        let Ok(cond_dict) = cond.cast::<PyDict>() else {
            continue;
        };
        if let Some((lon, lat, radius_rad)) = parse_field_geo_within_center_sphere(&cond_dict)? {
            let max_m = radius_rad * geo_s2::EARTH_RADIUS_METERS;
            return Ok(Some(QueryPlan::geo_within(
                name.clone(),
                field.to_string(),
                lon,
                lat,
                max_m,
            )));
        }
        if let Some(shape) = parse_field_geo_within_geometry(&cond_dict)? {
            let inner = cond_dict
                .get_item("$geoWithin")?
                .ok_or_else(|| PyRuntimeError::new_err("$geoWithin missing"))?;
            let inner_d = inner.cast::<PyDict>()?;
            let gt = geometry_type_label(&inner_d)?;
            return Ok(Some(QueryPlan::geo_within_geometry(
                name.clone(),
                field.to_string(),
                shape,
                gt,
            )));
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// RustQueryPlanner
// ---------------------------------------------------------------------------

/// Rust-native query planner -- chooses the best execution strategy and
/// executes index scans directly via WtCursor.
#[pyclass]
pub struct RustQueryPlanner {
    index_mgr: Py<RustIndexManager>,
}

// SAFETY: RustQueryPlanner holds only a Py<RustIndexManager> (no raw pointers).
// Send+Sync is required by PyO3 for #[pyclass].  The planner delegates all
// WiredTiger access to the index manager, which is protected by collection-level
// locks.  Safe under both GIL-enabled and free-threaded Python builds.
unsafe impl Send for RustQueryPlanner {}
unsafe impl Sync for RustQueryPlanner {}

impl RustQueryPlanner {
    pub(crate) fn new(index_mgr: Py<RustIndexManager>) -> Self {
        Self { index_mgr }
    }

    pub(crate) fn index_mgr_ref(&self, py: Python<'_>) -> Py<RustIndexManager> {
        self.index_mgr.clone_ref(py)
    }

    // -- planning ----------------------------------------------------------

    pub(crate) fn plan(&self, py: Python<'_>, query: &Bound<'_, PyDict>) -> PyResult<QueryPlan> {
        if query.is_empty() {
            return Ok(QueryPlan::collection_scan());
        }

        if let Some(id_val) = query.get_item("_id")? {
            if id_val.cast::<PyDict>().is_err() {
                return Ok(QueryPlan::pk_lookup());
            }
            if let Ok(id_dict) = id_val.cast::<PyDict>() {
                if id_dict.len() == 1 && id_dict.get_item("$eq")?.is_some() {
                    return Ok(QueryPlan::pk_lookup());
                }
            }
        }

        if query.get_item("$or")?.is_some() {
            return self.plan_or(py, query);
        }

        self.plan_simple(py, query)
    }

    fn plan_simple(&self, py: Python<'_>, query: &Bound<'_, PyDict>) -> PyResult<QueryPlan> {
        let field_conditions = collect_field_conditions(query)?;

        if field_conditions.is_empty() {
            return Ok(QueryPlan::collection_scan());
        }

        let mgr = self.index_mgr.bind(py).borrow();
        if let Some(gplan) = try_geo_near_plan(&mgr, &field_conditions)? {
            return Ok(gplan);
        }
        if let Some(gplan) = try_geo_intersects_plan(&mgr, &field_conditions)? {
            return Ok(gplan);
        }
        if let Some(gplan) = try_geo_within_plan(&mgr, &field_conditions)? {
            return Ok(gplan);
        }

        let mut best_plan: Option<QueryPlan> = None;
        let mut best_score: i32 = 0;
        let mut rejected: Vec<RejectedPlan> = Vec::new();

        for (name, idx) in mgr.indexes() {
            if idx.index_type != IndexType::Btree {
                continue;
            }
            let (score, bounds) = self.score_index(py, idx, &field_conditions)?;
            if score > best_score {
                if let Some(prev) = best_plan.take() {
                    if let Some(prev_name) = prev.index_name {
                        rejected.push(RejectedPlan {
                            index_name: prev_name,
                            score: best_score,
                        });
                    }
                }
                best_score = score;
                best_plan = Some(QueryPlan::index_scan(name.clone(), bounds));
            } else if score > 0 {
                rejected.push(RejectedPlan {
                    index_name: name.clone(),
                    score,
                });
            }
        }

        if let Some(mut plan) = best_plan {
            plan.rejected_plans = rejected;
            Ok(plan)
        } else {
            Ok(QueryPlan::collection_scan())
        }
    }

    fn plan_or(&self, py: Python<'_>, query: &Bound<'_, PyDict>) -> PyResult<QueryPlan> {
        let or_branches = query
            .get_item("$or")?
            .ok_or_else(|| PyRuntimeError::new_err("$or key missing"))?;

        let other_conditions = PyDict::new(py);
        for (k, v) in query.iter() {
            let ks: String = k.extract()?;
            if ks != "$or" {
                other_conditions.set_item(k, v)?;
            }
        }

        let mut subplans: Vec<QueryPlan> = Vec::new();
        for branch in or_branches.try_iter()? {
            let branch = branch?;
            let merged = other_conditions.copy()?;
            if let Ok(branch_dict) = branch.cast::<PyDict>() {
                for (k, v) in branch_dict.iter() {
                    merged.set_item(k, v)?;
                }
            }

            if let Some(id_val) = merged.get_item("_id")? {
                if id_val.cast::<PyDict>().is_err() {
                    subplans.push(QueryPlan::pk_lookup());
                    continue;
                }
            }

            let sub = self.plan_simple(py, &merged)?;
            if sub.plan_type == PlanType::CollectionScan {
                return Ok(QueryPlan::collection_scan());
            }
            subplans.push(sub);
        }

        if subplans.is_empty() {
            return Ok(QueryPlan::collection_scan());
        }
        Ok(QueryPlan::or_union(subplans))
    }

    fn score_index(
        &self,
        py: Python<'_>,
        idx: &IndexDef,
        field_conditions: &HashMap<String, Bound<'_, PyAny>>,
    ) -> PyResult<BoundsResult> {
        let mut score: i32 = 0;
        let mut lower_segments: Vec<BoundSegment> = Vec::new();
        let mut upper_segments: Vec<BoundSegment> = Vec::new();

        for (field, dir) in &idx.keys {
            let cond = match field_conditions.get(field.as_str()) {
                Some(c) => c,
                None => break,
            };

            if cond.cast::<PyDict>().is_err() {
                // Equality match
                score += 2;
                let mut encoded = sortable_encode_impl(py, cond)?;
                if *dir == IndexDir::Desc {
                    encoded = invert_encoded_impl(&encoded);
                }
                lower_segments.push(BoundSegment {
                    encoded: Some(encoded.clone()),
                    inclusive: true,
                });
                upper_segments.push(BoundSegment {
                    encoded: Some(encoded),
                    inclusive: true,
                });
            } else {
                let cond_dict = cond.cast::<PyDict>()?;
                let mut low_val: Option<Bound<'_, PyAny>> = None;
                let mut high_val: Option<Bound<'_, PyAny>> = None;
                let mut low_inc = false;
                let mut high_inc = false;
                let mut matched = false;

                for (op_key, val) in cond_dict.iter() {
                    let op: String = op_key.extract()?;
                    match op.as_str() {
                        "$gt" | "$gte" => {
                            low_val = Some(val.clone());
                            low_inc = op == "$gte";
                            matched = true;
                        }
                        "$lt" | "$lte" => {
                            high_val = Some(val.clone());
                            high_inc = op == "$lte";
                            matched = true;
                        }
                        "$eq" => {
                            low_val = Some(val.clone());
                            high_val = Some(val);
                            low_inc = true;
                            high_inc = true;
                            matched = true;
                        }
                        "$in" => {
                            matched = true;
                        }
                        _ => {}
                    }
                }

                if !matched {
                    break;
                }

                score += 1;

                if *dir == IndexDir::Asc {
                    let wt_low = match low_val {
                        Some(ref v) => Some(sortable_encode_impl(py, v)?),
                        None => None,
                    };
                    let wt_high = match high_val {
                        Some(ref v) => Some(sortable_encode_impl(py, v)?),
                        None => None,
                    };
                    lower_segments.push(BoundSegment {
                        encoded: wt_low,
                        inclusive: low_inc,
                    });
                    upper_segments.push(BoundSegment {
                        encoded: wt_high,
                        inclusive: high_inc,
                    });
                } else {
                    let wt_low = match high_val {
                        Some(ref v) => Some(invert_encoded_impl(&sortable_encode_impl(py, v)?)),
                        None => None,
                    };
                    let wt_high = match low_val {
                        Some(ref v) => Some(invert_encoded_impl(&sortable_encode_impl(py, v)?)),
                        None => None,
                    };
                    lower_segments.push(BoundSegment {
                        encoded: wt_low,
                        inclusive: high_inc,
                    });
                    upper_segments.push(BoundSegment {
                        encoded: wt_high,
                        inclusive: low_inc,
                    });
                }

                let vals_equal = match (&low_val, &high_val) {
                    (Some(l), Some(h)) => l.eq(h)?,
                    _ => false,
                };
                if !vals_equal {
                    break;
                }
            }
        }

        Ok((score, (lower_segments, upper_segments)))
    }

    // -- index scan execution ----------------------------------------------

    pub(crate) fn execute_index_scan(
        &self,
        py: Python<'_>,
        plan: &QueryPlan,
        session_raw: Option<*mut wiredtiger_sys::WT_SESSION>,
    ) -> PyResult<Vec<String>> {
        let idx_name = plan
            .index_def_name
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("index_scan plan has no index_def_name"))?;
        let mgr = self.index_mgr.bind(py).borrow();
        let idx = mgr
            .indexes()
            .get(idx_name)
            .ok_or_else(|| PyRuntimeError::new_err(format!("index {idx_name} not found")))?;
        let table_uri = idx
            .table_uri
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err(format!("index {idx_name} has no table_uri")))?;

        let (ref lower_segs, ref upper_segs) = plan
            .bounds
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("index_scan plan has no bounds"))?;

        let low_key = build_bound_key(lower_segs, true);
        let high_key = build_bound_key(upper_segs, false);

        let session = borrow_session(session_raw)?;
        let mut cursor = session.open_cursor(table_uri, None).py()?;
        let mut ids: Vec<String> = Vec::new();

        if let Some(ref lk) = low_key {
            cursor.set_key_str(lk);
            match cursor.search_near() {
                Ok(exact) => {
                    if exact < 0 && cursor.next().is_err() {
                        cursor.close().py()?;
                        return Ok(ids);
                    }
                }
                Err(_) => {
                    cursor.close().py()?;
                    return Ok(ids);
                }
            }
        } else if cursor.next().is_err() {
            cursor.close().py()?;
            return Ok(ids);
        }

        loop {
            let key = cursor.get_key_str().py()?;
            if let Some(ref hk) = high_key {
                if key.as_str() > hk.as_str() {
                    break;
                }
            }
            ids.push(cursor.get_value_str().py()?);
            if cursor.next().is_err() {
                break;
            }
        }

        cursor.close().py()?;
        Ok(ids)
    }

    pub(crate) fn execute_in_scan(
        &self,
        py: Python<'_>,
        idx_name: &str,
        values: &Bound<'_, PyAny>,
        session_raw: Option<*mut wiredtiger_sys::WT_SESSION>,
    ) -> PyResult<Vec<String>> {
        let mgr = self.index_mgr.bind(py).borrow();
        let idx = mgr
            .indexes()
            .get(idx_name)
            .ok_or_else(|| PyRuntimeError::new_err(format!("index {idx_name} not found")))?;
        let table_uri = idx
            .table_uri
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err(format!("index {idx_name} has no table_uri")))?;

        let first_dir = idx.keys.first().map(|(_, d)| *d).unwrap_or(IndexDir::Asc);

        let session = borrow_session(session_raw)?;
        let mut cursor = session.open_cursor(table_uri, None).py()?;
        let mut seen: HashSet<String> = HashSet::new();
        let mut ids: Vec<String> = Vec::new();

        for val in values.try_iter()? {
            let val = val?;
            let mut encoded = sortable_encode_impl(py, &val)?;
            if first_dir == IndexDir::Desc {
                encoded = invert_encoded_impl(&encoded);
            }
            let prefix = format!("{encoded}|");
            cursor.set_key_str(&prefix);
            match cursor.search_near() {
                Ok(exact) => {
                    if exact < 0 && cursor.next().is_err() {
                        continue;
                    }
                }
                Err(_) => continue,
            }
            loop {
                let key = cursor.get_key_str().py()?;
                if !key.starts_with(&prefix) {
                    break;
                }
                let doc_id = cursor.get_value_str().py()?;
                if seen.insert(doc_id.clone()) {
                    ids.push(doc_id);
                }
                if cursor.next().is_err() {
                    break;
                }
            }
        }

        cursor.close().py()?;
        Ok(ids)
    }
}

// ---------------------------------------------------------------------------
// #[pymethods] wrappers for Python callers
// ---------------------------------------------------------------------------

#[pymethods]
impl RustQueryPlanner {
    #[new]
    fn py_new(index_mgr: Py<RustIndexManager>) -> Self {
        Self::new(index_mgr)
    }

    #[pyo3(name = "plan")]
    fn py_plan<'py>(&self, py: Python<'py>, query: &Bound<'py, PyDict>) -> PyResult<Py<PyDict>> {
        let plan = self.plan(py, query)?;
        plan.to_py_dict(py)
    }

    #[pyo3(name = "execute_index_scan")]
    fn py_execute_index_scan(
        &self,
        py: Python<'_>,
        plan_dict: &Bound<'_, PyAny>,
        session: &Bound<'_, PyAny>,
        _table_uri: &str,
    ) -> PyResult<Vec<String>> {
        // Python callers pass the old-style Python QueryPlan object.
        // Extract enough info to build a Rust QueryPlan.
        let plan_type: String = plan_dict.getattr("plan_type")?.extract()?;
        if plan_type != "index_scan" {
            return Ok(Vec::new());
        }
        let index_name: String = plan_dict.getattr("index_name")?.extract()?;
        let bounds_obj = plan_dict.getattr("bounds")?;
        let (lower, upper) = extract_bounds_from_py(py, &bounds_obj)?;

        let plan = QueryPlan {
            plan_type: PlanType::IndexScan,
            index_name: Some(index_name.clone()),
            bounds: Some((lower, upper)),
            index_def_name: Some(index_name),
            subplans: None,
            rejected_plans: Vec::new(),
            geo_field: None,
            geo_lon: None,
            geo_lat: None,
            geo_max_distance_m: None,
            geo_min_distance_m: None,
            geo_cover_cells: None,
            geo_shape: None,
            geo_geometry_type: None,
        };

        let session_raw = extract_session_raw(py, session)?;
        self.execute_index_scan(py, &plan, session_raw)
    }

    #[pyo3(name = "execute_in_scan")]
    fn py_execute_in_scan(
        &self,
        py: Python<'_>,
        idx_def: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        session: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<String>> {
        let idx_name: String = idx_def.getattr("name")?.extract()?;
        let session_raw = extract_session_raw(py, session)?;
        self.execute_in_scan(py, &idx_name, values, session_raw)
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

fn borrow_session(
    session_raw: Option<*mut wiredtiger_sys::WT_SESSION>,
) -> PyResult<ManuallyDrop<WtSession>> {
    crate::wt_bridge::borrow_wt_session(session_raw, "query planner")
}

pub(crate) fn build_bound_key(segments: &[BoundSegment], is_lower: bool) -> Option<String> {
    if segments.is_empty() {
        return None;
    }

    let mut parts: Vec<String> = Vec::with_capacity(segments.len());
    for seg in segments {
        match &seg.encoded {
            Some(enc) => parts.push(enc.clone()),
            None => {
                if is_lower {
                    parts.push(String::new());
                } else {
                    parts.push("\u{00ff}".repeat(20));
                }
            }
        }
    }

    let mut key = parts.join("|");
    if is_lower {
        key.push('|');
    } else {
        key.push('|');
        key.push_str(&"\u{00ff}".repeat(40));
    }
    Some(key)
}

fn extract_session_raw(
    py: Python<'_>,
    session: &Bound<'_, PyAny>,
) -> PyResult<Option<*mut wiredtiger_sys::WT_SESSION>> {
    if let Ok(rs) = session.extract::<Py<crate::wt_bridge::RustWtSession>>() {
        let bound = rs.bind(py).borrow();
        Ok(Some(bound.get().py()?.raw_ptr()))
    } else {
        Err(PyRuntimeError::new_err(
            "QueryPlanner requires RustWtSession",
        ))
    }
}

fn extract_bounds_from_py(
    _py: Python<'_>,
    bounds_obj: &Bound<'_, PyAny>,
) -> PyResult<(Vec<BoundSegment>, Vec<BoundSegment>)> {
    if bounds_obj.is_none() {
        return Ok((Vec::new(), Vec::new()));
    }
    let pair = bounds_obj.extract::<(Bound<'_, PyAny>, Bound<'_, PyAny>)>()?;
    let lower = extract_segment_list(&pair.0)?;
    let upper = extract_segment_list(&pair.1)?;
    Ok((lower, upper))
}

fn extract_segment_list(list_obj: &Bound<'_, PyAny>) -> PyResult<Vec<BoundSegment>> {
    let mut out = Vec::new();
    for item in list_obj.try_iter()? {
        let item = item?;
        let pair = item.extract::<(Bound<'_, PyAny>, bool)>()?;
        let encoded = if pair.0.is_none() {
            None
        } else {
            Some(pair.0.extract::<String>()?)
        };
        out.push(BoundSegment {
            encoded,
            inclusive: pair.1,
        });
    }
    Ok(out)
}
