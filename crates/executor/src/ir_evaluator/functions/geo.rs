use geo::algorithm::GeodesicArea;
use geo::algorithm::area::Area;
use geo::algorithm::bounding_rect::BoundingRect;
use geo::algorithm::centroid::Centroid;
use geo::algorithm::convex_hull::ConvexHull;
use geo::algorithm::geodesic_distance::GeodesicDistance;
use geo::algorithm::geodesic_length::GeodesicLength;
use geo::algorithm::intersects::Intersects;
use geo::algorithm::simplify_vw::SimplifyVw;
use geo::{BooleanOps, Contains, Coord, Geometry, LineString, MultiPolygon, Point, Polygon};
use ordered_float::OrderedFloat;
use wkt::TryFromWkt;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::super::IrEvaluator;

impl<'a> IrEvaluator<'a> {
    pub(crate) fn fn_st_geogfromtext(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(wkt) => Ok(Value::Geography(wkt.clone())),
            _ => Err(Error::InvalidQuery(
                "ST_GEOGFROMTEXT expects a string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_geogpoint(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_GEOGPOINT requires longitude and latitude".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Float64(lon), Value::Float64(lat)) => {
                Ok(Value::Geography(format!("POINT({} {})", lon.0, lat.0)))
            }
            (Value::Int64(lon), Value::Int64(lat)) => {
                Ok(Value::Geography(format!("POINT({} {})", lon, lat)))
            }
            (Value::Float64(lon), Value::Int64(lat)) => {
                Ok(Value::Geography(format!("POINT({} {})", lon.0, lat)))
            }
            (Value::Int64(lon), Value::Float64(lat)) => {
                Ok(Value::Geography(format!("POINT({} {})", lon, lat.0)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_GEOGPOINT expects numeric arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_st_geogfromgeojson(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(geojson) => {
                let parsed: serde_json::Value = serde_json::from_str(geojson)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid GeoJSON: {}", e)))?;
                let geom_type = parsed.get("type").and_then(|t| t.as_str());
                let coords = parsed.get("coordinates");
                match (geom_type, coords) {
                    (Some("Point"), Some(serde_json::Value::Array(c))) if c.len() >= 2 => {
                        let x = c[0].as_f64().unwrap_or(0.0);
                        let y = c[1].as_f64().unwrap_or(0.0);
                        Ok(Value::Geography(format!("POINT({} {})", x, y)))
                    }
                    _ => Ok(Value::Geography(geojson.clone())),
                }
            }
            _ => Err(Error::InvalidQuery(
                "ST_GEOGFROMGEOJSON expects a string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_astext(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => Ok(Value::String(wkt.clone())),
            _ => Err(Error::InvalidQuery(
                "ST_ASTEXT expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_asgeojson(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                if wkt.starts_with("POINT(") && wkt.ends_with(")") {
                    let inner = &wkt[6..wkt.len() - 1];
                    let parts: Vec<&str> = inner.split_whitespace().collect();
                    if parts.len() >= 2 {
                        let x: f64 = parts[0].parse().unwrap_or(0.0);
                        let y: f64 = parts[1].parse().unwrap_or(0.0);
                        return Ok(Value::String(format!(
                            "{{\"type\":\"Point\",\"coordinates\":[{},{}]}}",
                            x, y
                        )));
                    }
                }
                Ok(Value::String(format!("{{\"wkt\":\"{}\"}}", wkt)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_ASGEOJSON expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_asbinary(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => Ok(Value::Bytes(wkt.as_bytes().to_vec())),
            _ => Err(Error::InvalidQuery(
                "ST_ASBINARY expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_x(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                if wkt.starts_with("POINT(") && wkt.ends_with(")") {
                    let inner = &wkt[6..wkt.len() - 1];
                    let parts: Vec<&str> = inner.split_whitespace().collect();
                    if let Some(x_str) = parts.first() {
                        if let Ok(x) = x_str.parse::<f64>() {
                            return Ok(Value::Float64(OrderedFloat(x)));
                        }
                    }
                }
                Ok(Value::Null)
            }
            _ => Err(Error::InvalidQuery(
                "ST_X expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_y(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                if wkt.starts_with("POINT(") && wkt.ends_with(")") {
                    let inner = &wkt[6..wkt.len() - 1];
                    let parts: Vec<&str> = inner.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(y) = parts[1].parse::<f64>() {
                            return Ok(Value::Float64(OrderedFloat(y)));
                        }
                    }
                }
                Ok(Value::Null)
            }
            _ => Err(Error::InvalidQuery(
                "ST_Y expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_area(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let area = geom.geodesic_area_signed().abs();
                Ok(Value::Float64(OrderedFloat(area)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_AREA expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_length(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let length = match &geom {
                    Geometry::LineString(ls) => ls.geodesic_length(),
                    Geometry::MultiLineString(mls) => {
                        mls.0.iter().map(|ls| ls.geodesic_length()).sum()
                    }
                    _ => 0.0,
                };
                Ok(Value::Float64(OrderedFloat(length)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_LENGTH expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_perimeter(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let perimeter = match &geom {
                    Geometry::Polygon(poly) => poly.exterior().geodesic_length(),
                    Geometry::MultiPolygon(mp) => {
                        mp.0.iter().map(|p| p.exterior().geodesic_length()).sum()
                    }
                    _ => 0.0,
                };
                Ok(Value::Float64(OrderedFloat(perimeter)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_PERIMETER expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_distance(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_DISTANCE requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let distance = Self::geodesic_distance_between_geometries(&geom1, &geom2);
                Ok(Value::Float64(OrderedFloat(distance)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_DISTANCE expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn geodesic_distance_between_geometries(
        geom1: &Geometry<f64>,
        geom2: &Geometry<f64>,
    ) -> f64 {
        match (geom1, geom2) {
            (Geometry::Point(p1), Geometry::Point(p2)) => p1.geodesic_distance(p2),
            (Geometry::Point(p), Geometry::LineString(l))
            | (Geometry::LineString(l), Geometry::Point(p)) => l
                .points()
                .map(|lp| p.geodesic_distance(&lp))
                .fold(f64::INFINITY, f64::min),
            (Geometry::Point(p), Geometry::Polygon(poly))
            | (Geometry::Polygon(poly), Geometry::Point(p)) => poly
                .exterior()
                .points()
                .map(|pp| p.geodesic_distance(&pp))
                .fold(f64::INFINITY, f64::min),
            (Geometry::LineString(l1), Geometry::LineString(l2)) => l1
                .points()
                .flat_map(|p1| l2.points().map(move |p2| p1.geodesic_distance(&p2)))
                .fold(f64::INFINITY, f64::min),
            (Geometry::Polygon(poly1), Geometry::Polygon(poly2)) => poly1
                .exterior()
                .points()
                .flat_map(|p1| {
                    poly2
                        .exterior()
                        .points()
                        .map(move |p2| p1.geodesic_distance(&p2))
                })
                .fold(f64::INFINITY, f64::min),
            _ => 0.0,
        }
    }

    pub(crate) fn fn_st_centroid(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let centroid = geom.centroid();
                match centroid {
                    Some(p) => Ok(Value::Geography(format!("POINT({} {})", p.x(), p.y()))),
                    None => Ok(Value::Null),
                }
            }
            _ => Err(Error::InvalidQuery(
                "ST_CENTROID expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_buffer(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Ok(Value::Null);
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt), Value::Float64(_))
            | (Value::Geography(wkt), Value::Int64(_)) => {
                let distance_meters = match &args[1] {
                    Value::Float64(f) => f.0,
                    Value::Int64(i) => *i as f64,
                    _ => 0.0,
                };
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let buffered = Self::create_buffer(&geom, distance_meters);
                Ok(Value::Geography(Self::geometry_to_wkt(&buffered)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_BUFFER expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn create_buffer(geom: &Geometry<f64>, distance_meters: f64) -> Geometry<f64> {
        match geom {
            Geometry::Point(p) => {
                let num_segments = 32;
                let deg_per_meter_lat = 1.0 / 111_320.0;
                let deg_per_meter_lon = 1.0 / (111_320.0 * p.y().to_radians().cos());
                let mut coords = Vec::with_capacity(num_segments + 1);
                for i in 0..num_segments {
                    let angle = 2.0 * std::f64::consts::PI * (i as f64) / (num_segments as f64);
                    let dx = distance_meters * angle.cos() * deg_per_meter_lon;
                    let dy = distance_meters * angle.sin() * deg_per_meter_lat;
                    coords.push(Coord {
                        x: p.x() + dx,
                        y: p.y() + dy,
                    });
                }
                coords.push(coords[0]);
                Geometry::Polygon(Polygon::new(LineString::new(coords), vec![]))
            }
            _ => geom.clone(),
        }
    }

    pub(crate) fn geometry_to_wkt(geom: &Geometry<f64>) -> String {
        use std::fmt::Write;
        match geom {
            Geometry::Point(p) => format!("POINT({} {})", p.x(), p.y()),
            Geometry::LineString(ls) => {
                let coords: Vec<String> = ls.coords().map(|c| format!("{} {}", c.x, c.y)).collect();
                format!("LINESTRING({})", coords.join(", "))
            }
            Geometry::Polygon(poly) => {
                let exterior: Vec<String> = poly
                    .exterior()
                    .coords()
                    .map(|c| format!("{} {}", c.x, c.y))
                    .collect();
                if poly.interiors().is_empty() {
                    format!("POLYGON(({}))", exterior.join(", "))
                } else {
                    let mut result = format!("POLYGON(({})", exterior.join(", "));
                    for interior in poly.interiors() {
                        let interior_coords: Vec<String> = interior
                            .coords()
                            .map(|c| format!("{} {}", c.x, c.y))
                            .collect();
                        let _ = write!(result, ", ({})", interior_coords.join(", "));
                    }
                    result.push(')');
                    result
                }
            }
            Geometry::MultiPoint(mp) => {
                let points: Vec<String> =
                    mp.0.iter()
                        .map(|p| format!("{} {}", p.x(), p.y()))
                        .collect();
                format!("MULTIPOINT({})", points.join(", "))
            }
            Geometry::MultiLineString(mls) => {
                let lines: Vec<String> = mls
                    .0
                    .iter()
                    .map(|ls| {
                        let coords: Vec<String> =
                            ls.coords().map(|c| format!("{} {}", c.x, c.y)).collect();
                        format!("({})", coords.join(", "))
                    })
                    .collect();
                format!("MULTILINESTRING({})", lines.join(", "))
            }
            Geometry::MultiPolygon(mp) => {
                let polys: Vec<String> =
                    mp.0.iter()
                        .map(|poly| {
                            let exterior: Vec<String> = poly
                                .exterior()
                                .coords()
                                .map(|c| format!("{} {}", c.x, c.y))
                                .collect();
                            format!("(({}))", exterior.join(", "))
                        })
                        .collect();
                format!("MULTIPOLYGON({})", polys.join(", "))
            }
            Geometry::GeometryCollection(gc) => {
                if gc.0.is_empty() {
                    "GEOMETRYCOLLECTION EMPTY".to_string()
                } else {
                    let geoms: Vec<String> = gc.0.iter().map(Self::geometry_to_wkt).collect();
                    format!("GEOMETRYCOLLECTION({})", geoms.join(", "))
                }
            }
            _ => "GEOMETRYCOLLECTION EMPTY".to_string(),
        }
    }

    pub(crate) fn fn_st_boundingbox(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let rect = geom.bounding_rect();
                match rect {
                    Some(r) => {
                        let min = r.min();
                        let max = r.max();
                        let bbox_wkt = format!(
                            "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))",
                            min.x, min.y, min.x, max.y, max.x, max.y, max.x, min.y, min.x, min.y
                        );
                        Ok(Value::Geography(bbox_wkt))
                    }
                    None => Ok(Value::Null),
                }
            }
            _ => Err(Error::InvalidQuery(
                "ST_BOUNDINGBOX expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_closestpoint(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_CLOSESTPOINT requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let closest = Self::find_closest_point(&geom1, &geom2);
                Ok(Value::Geography(format!(
                    "POINT({} {})",
                    closest.x(),
                    closest.y()
                )))
            }
            _ => Err(Error::InvalidQuery(
                "ST_CLOSESTPOINT expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn find_closest_point(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> Point<f64> {
        let target_point = match geom2 {
            Geometry::Point(p) => *p,
            _ => geom2.centroid().unwrap_or(Point::new(0.0, 0.0)),
        };

        let points = Self::extract_points(geom1);
        if points.is_empty() {
            return Point::new(0.0, 0.0);
        }

        points
            .into_iter()
            .min_by(|a, b| {
                let da = a.geodesic_distance(&target_point);
                let db = b.geodesic_distance(&target_point);
                da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap_or(Point::new(0.0, 0.0))
    }

    pub(crate) fn extract_points(geom: &Geometry<f64>) -> Vec<Point<f64>> {
        match geom {
            Geometry::Point(p) => vec![*p],
            Geometry::LineString(ls) => ls.points().collect(),
            Geometry::Polygon(poly) => poly.exterior().points().collect(),
            Geometry::MultiPoint(mp) => mp.0.clone(),
            Geometry::MultiLineString(mls) => mls.0.iter().flat_map(|ls| ls.points()).collect(),
            Geometry::MultiPolygon(mp) => {
                mp.0.iter()
                    .flat_map(|poly| poly.exterior().points())
                    .collect()
            }
            _ => vec![],
        }
    }

    pub(crate) fn fn_st_contains(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_CONTAINS requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_contains(&geom1, &geom2);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_CONTAINS expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn geometry_contains(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> bool {
        match (geom1, geom2) {
            (Geometry::Polygon(poly), Geometry::Point(p)) => poly.contains(p),
            (Geometry::Polygon(poly1), Geometry::Polygon(poly2)) => {
                poly2.exterior().points().all(|p| poly1.contains(&p))
            }
            (Geometry::Polygon(poly), Geometry::LineString(ls)) => {
                ls.points().all(|p| poly.contains(&p))
            }
            (Geometry::MultiPolygon(mp), Geometry::Point(p)) => {
                mp.0.iter().any(|poly| poly.contains(p))
            }
            _ => false,
        }
    }

    pub(crate) fn fn_st_intersects(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_INTERSECTS requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = geom1.intersects(&geom2);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_INTERSECTS expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_st_union(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_UNION requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_union(&geom1, &geom2);
                Ok(Value::Geography(Self::geometry_to_wkt(&result)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_UNION expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn geometry_union(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> Geometry<f64> {
        match (geom1, geom2) {
            (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
                let mp1 = MultiPolygon::new(vec![p1.clone()]);
                let mp2 = MultiPolygon::new(vec![p2.clone()]);
                Geometry::MultiPolygon(mp1.union(&mp2))
            }
            (Geometry::MultiPolygon(mp1), Geometry::Polygon(p2)) => {
                let mp2 = MultiPolygon::new(vec![p2.clone()]);
                Geometry::MultiPolygon(mp1.union(&mp2))
            }
            (Geometry::Polygon(p1), Geometry::MultiPolygon(mp2)) => {
                let mp1 = MultiPolygon::new(vec![p1.clone()]);
                Geometry::MultiPolygon(mp1.union(mp2))
            }
            (Geometry::MultiPolygon(mp1), Geometry::MultiPolygon(mp2)) => {
                Geometry::MultiPolygon(mp1.union(mp2))
            }
            _ => geom1.clone(),
        }
    }

    pub(crate) fn fn_st_intersection(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_INTERSECTION requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_intersection(&geom1, &geom2);
                Ok(Value::Geography(Self::geometry_to_wkt(&result)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_INTERSECTION expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn geometry_intersection(
        geom1: &Geometry<f64>,
        geom2: &Geometry<f64>,
    ) -> Geometry<f64> {
        match (geom1, geom2) {
            (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
                let mp1 = MultiPolygon::new(vec![p1.clone()]);
                let mp2 = MultiPolygon::new(vec![p2.clone()]);
                Geometry::MultiPolygon(mp1.intersection(&mp2))
            }
            (Geometry::MultiPolygon(mp1), Geometry::Polygon(p2)) => {
                let mp2 = MultiPolygon::new(vec![p2.clone()]);
                Geometry::MultiPolygon(mp1.intersection(&mp2))
            }
            (Geometry::Polygon(p1), Geometry::MultiPolygon(mp2)) => {
                let mp1 = MultiPolygon::new(vec![p1.clone()]);
                Geometry::MultiPolygon(mp1.intersection(mp2))
            }
            (Geometry::MultiPolygon(mp1), Geometry::MultiPolygon(mp2)) => {
                Geometry::MultiPolygon(mp1.intersection(mp2))
            }
            _ => Geometry::GeometryCollection(geo_types::GeometryCollection::new_from(vec![])),
        }
    }

    pub(crate) fn fn_st_difference(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_DIFFERENCE requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_difference(&geom1, &geom2);
                Ok(Value::Geography(Self::geometry_to_wkt(&result)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_DIFFERENCE expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn geometry_difference(
        geom1: &Geometry<f64>,
        geom2: &Geometry<f64>,
    ) -> Geometry<f64> {
        match (geom1, geom2) {
            (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
                let mp1 = MultiPolygon::new(vec![p1.clone()]);
                let mp2 = MultiPolygon::new(vec![p2.clone()]);
                Geometry::MultiPolygon(mp1.difference(&mp2))
            }
            (Geometry::MultiPolygon(mp1), Geometry::Polygon(p2)) => {
                let mp2 = MultiPolygon::new(vec![p2.clone()]);
                Geometry::MultiPolygon(mp1.difference(&mp2))
            }
            (Geometry::Polygon(p1), Geometry::MultiPolygon(mp2)) => {
                let mp1 = MultiPolygon::new(vec![p1.clone()]);
                Geometry::MultiPolygon(mp1.difference(mp2))
            }
            (Geometry::MultiPolygon(mp1), Geometry::MultiPolygon(mp2)) => {
                Geometry::MultiPolygon(mp1.difference(mp2))
            }
            _ => geom1.clone(),
        }
    }

    pub(crate) fn fn_st_makeline(&self, args: &[Value]) -> Result<Value> {
        let mut points = Vec::new();

        if args.len() == 1 {
            if let Value::Array(arr) = &args[0] {
                for elem in arr {
                    if let Value::Geography(wkt) = elem {
                        if wkt.starts_with("POINT(") {
                            let inner = &wkt[6..wkt.len() - 1];
                            points.push(inner.to_string());
                        }
                    }
                }
            }
        } else {
            for arg in args {
                match arg {
                    Value::Null => continue,
                    Value::Geography(wkt) if wkt.starts_with("POINT(") => {
                        let inner = &wkt[6..wkt.len() - 1];
                        points.push(inner.to_string());
                    }
                    _ => {}
                }
            }
        }

        if points.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_MAKELINE requires at least two geography points".into(),
            ));
        }
        Ok(Value::Geography(format!(
            "LINESTRING({})",
            points.join(", ")
        )))
    }

    pub(crate) fn fn_st_makepolygon(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) if wkt.starts_with("LINESTRING(") => {
                let inner = &wkt[11..wkt.len() - 1];
                Ok(Value::Geography(format!("POLYGON(({}))", inner)))
            }
            Value::Geography(wkt) => Ok(Value::Geography(wkt.clone())),
            _ => Err(Error::InvalidQuery(
                "ST_MAKEPOLYGON expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_numpoints(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                if wkt.starts_with("POINT(") {
                    Ok(Value::Int64(1))
                } else if wkt.starts_with("LINESTRING(") || wkt.starts_with("POLYGON(") {
                    let count = wkt.matches(',').count() + 1;
                    Ok(Value::Int64(count as i64))
                } else {
                    Ok(Value::Int64(0))
                }
            }
            _ => Err(Error::InvalidQuery(
                "ST_NUMPOINTS expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_isclosed(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let is_closed = match &geom {
                    Geometry::Point(_) => true,
                    Geometry::LineString(ls) => {
                        if ls.0.len() < 2 {
                            false
                        } else {
                            ls.0.first() == ls.0.last()
                        }
                    }
                    Geometry::Polygon(_) => true,
                    _ => false,
                };
                Ok(Value::Bool(is_closed))
            }
            _ => Err(Error::InvalidQuery(
                "ST_ISCLOSED expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_isempty(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let is_empty = wkt.contains("EMPTY") || wkt.is_empty();
                Ok(Value::Bool(is_empty))
            }
            _ => Err(Error::InvalidQuery(
                "ST_ISEMPTY expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_geometrytype(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom_type = if wkt.starts_with("POINT") {
                    "Point"
                } else if wkt.starts_with("LINESTRING") {
                    "LineString"
                } else if wkt.starts_with("POLYGON") {
                    "Polygon"
                } else if wkt.starts_with("MULTIPOINT") {
                    "MultiPoint"
                } else if wkt.starts_with("MULTILINESTRING") {
                    "MultiLineString"
                } else if wkt.starts_with("MULTIPOLYGON") {
                    "MultiPolygon"
                } else if wkt.starts_with("GEOMETRYCOLLECTION") {
                    "GeometryCollection"
                } else {
                    "Unknown"
                };
                Ok(Value::String(geom_type.to_string()))
            }
            _ => Err(Error::InvalidQuery(
                "ST_GEOMETRYTYPE expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_dimension(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let dim = if wkt.starts_with("POINT") {
                    0
                } else if wkt.starts_with("LINESTRING") || wkt.starts_with("MULTILINESTRING") {
                    1
                } else if wkt.starts_with("POLYGON") || wkt.starts_with("MULTIPOLYGON") {
                    2
                } else {
                    -1
                };
                Ok(Value::Int64(dim))
            }
            _ => Err(Error::InvalidQuery(
                "ST_DIMENSION expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_within(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_WITHIN requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_contains(&geom2, &geom1);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_WITHIN expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_st_dwithin(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "ST_DWITHIN requires two geography arguments and a distance".into(),
            ));
        }
        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2), distance_val) => {
                let distance_limit = match distance_val {
                    Value::Float64(f) => f.0,
                    Value::Int64(i) => *i as f64,
                    _ => return Ok(Value::Bool(false)),
                };
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let distance = Self::geodesic_distance_between_geometries(&geom1, &geom2);
                Ok(Value::Bool(distance <= distance_limit))
            }
            _ => Err(Error::InvalidQuery(
                "ST_DWITHIN expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_st_covers(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_COVERS requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_contains(&geom1, &geom2);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_COVERS expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_st_coveredby(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_COVEREDBY requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_contains(&geom2, &geom1);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_COVEREDBY expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_st_touches(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_TOUCHES requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_touches(&geom1, &geom2);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_TOUCHES expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn geometry_touches(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> bool {
        match (geom1, geom2) {
            (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
                let points1: Vec<Point<f64>> = p1.exterior().points().collect();
                let points2: Vec<Point<f64>> = p2.exterior().points().collect();
                for p in &points1 {
                    if points2
                        .iter()
                        .any(|p2| (p.x() - p2.x()).abs() < 1e-10 && (p.y() - p2.y()).abs() < 1e-10)
                    {
                        return true;
                    }
                }
                false
            }
            _ => false,
        }
    }

    pub(crate) fn fn_st_disjoint(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_DISJOINT requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = !geom1.intersects(&geom2);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_DISJOINT expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_st_equals(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_EQUALS requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(a), Value::Geography(b)) => Ok(Value::Bool(a == b)),
            _ => Err(Error::InvalidQuery(
                "ST_EQUALS expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_st_convexhull(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let hull = geom.convex_hull();
                Ok(Value::Geography(Self::geometry_to_wkt(&Geometry::Polygon(
                    hull,
                ))))
            }
            _ => Err(Error::InvalidQuery(
                "ST_CONVEXHULL expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_simplify(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Ok(Value::Null);
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) => Ok(Value::Null),
            (Value::Geography(wkt), tolerance_val) => {
                let epsilon = match tolerance_val {
                    Value::Float64(f) => f.0,
                    Value::Int64(i) => *i as f64,
                    _ => 0.0,
                };
                let epsilon_degrees = epsilon / 111_320.0;
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let simplified = match geom {
                    Geometry::LineString(ls) => {
                        let simplified = ls.simplify_vw(&epsilon_degrees);
                        Geometry::LineString(simplified)
                    }
                    Geometry::Polygon(poly) => {
                        let simplified_exterior = poly.exterior().simplify_vw(&epsilon_degrees);
                        Geometry::Polygon(Polygon::new(simplified_exterior, vec![]))
                    }
                    other => other,
                };
                Ok(Value::Geography(Self::geometry_to_wkt(&simplified)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_SIMPLIFY expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_snaptogrid(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Ok(Value::Null);
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) => Ok(Value::Null),
            (Value::Geography(wkt), grid_val) => {
                let grid_size = match grid_val {
                    Value::Float64(f) => f.0,
                    Value::Int64(i) => *i as f64,
                    _ => 1.0,
                };
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let snapped = Self::snap_geometry_to_grid(&geom, grid_size);
                Ok(Value::Geography(Self::geometry_to_wkt(&snapped)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_SNAPTOGRID expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn snap_geometry_to_grid(geom: &Geometry<f64>, grid_size: f64) -> Geometry<f64> {
        let snap = |v: f64| -> f64 { (v / grid_size).round() * grid_size };
        match geom {
            Geometry::Point(p) => Geometry::Point(Point::new(snap(p.x()), snap(p.y()))),
            Geometry::LineString(ls) => {
                let coords: Vec<Coord<f64>> = ls
                    .coords()
                    .map(|c| Coord {
                        x: snap(c.x),
                        y: snap(c.y),
                    })
                    .collect();
                Geometry::LineString(LineString::new(coords))
            }
            Geometry::Polygon(poly) => {
                let exterior: Vec<Coord<f64>> = poly
                    .exterior()
                    .coords()
                    .map(|c| Coord {
                        x: snap(c.x),
                        y: snap(c.y),
                    })
                    .collect();
                Geometry::Polygon(Polygon::new(LineString::new(exterior), vec![]))
            }
            other => other.clone(),
        }
    }

    pub(crate) fn fn_st_boundary(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let boundary = match geom {
                    Geometry::Polygon(poly) => Geometry::LineString(poly.exterior().clone()),
                    Geometry::LineString(ls) => {
                        if ls.0.len() >= 2 {
                            let start = ls.0.first().unwrap();
                            let end = ls.0.last().unwrap();
                            if start == end {
                                Geometry::GeometryCollection(
                                    geo_types::GeometryCollection::new_from(vec![]),
                                )
                            } else {
                                Geometry::MultiPoint(geo_types::MultiPoint::new(vec![
                                    Point::new(start.x, start.y),
                                    Point::new(end.x, end.y),
                                ]))
                            }
                        } else {
                            Geometry::GeometryCollection(geo_types::GeometryCollection::new_from(
                                vec![],
                            ))
                        }
                    }
                    _ => Geometry::GeometryCollection(geo_types::GeometryCollection::new_from(
                        vec![],
                    )),
                };
                Ok(Value::Geography(Self::geometry_to_wkt(&boundary)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_BOUNDARY expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_startpoint(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) if wkt.starts_with("LINESTRING(") => {
                let inner = &wkt[11..wkt.len() - 1];
                if let Some(first_comma) = inner.find(',') {
                    let first_point = inner[..first_comma].trim();
                    Ok(Value::Geography(format!("POINT({})", first_point)))
                } else {
                    Ok(Value::Geography(format!("POINT({})", inner.trim())))
                }
            }
            Value::Geography(_) => Ok(Value::Null),
            _ => Err(Error::InvalidQuery(
                "ST_STARTPOINT expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_endpoint(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) if wkt.starts_with("LINESTRING(") => {
                let inner = &wkt[11..wkt.len() - 1];
                if let Some(last_comma) = inner.rfind(',') {
                    let last_point = inner[last_comma + 1..].trim();
                    Ok(Value::Geography(format!("POINT({})", last_point)))
                } else {
                    Ok(Value::Geography(format!("POINT({})", inner.trim())))
                }
            }
            Value::Geography(_) => Ok(Value::Null),
            _ => Err(Error::InvalidQuery(
                "ST_ENDPOINT expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_pointn(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_POINTN requires a geography and an index".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt), Value::Int64(n)) if wkt.starts_with("LINESTRING(") => {
                let inner = &wkt[11..wkt.len() - 1];
                let points: Vec<&str> = inner.split(',').collect();
                let idx = (*n - 1) as usize;
                if idx < points.len() {
                    Ok(Value::Geography(format!("POINT({})", points[idx].trim())))
                } else {
                    Ok(Value::Null)
                }
            }
            (Value::Geography(_), Value::Int64(_)) => Ok(Value::Null),
            _ => Err(Error::InvalidQuery(
                "ST_POINTN expects geography and integer arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_st_iscollection(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let is_collection =
                    wkt.starts_with("MULTI") || wkt.starts_with("GEOMETRYCOLLECTION");
                Ok(Value::Bool(is_collection))
            }
            _ => Err(Error::InvalidQuery(
                "ST_ISCOLLECTION expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_isring(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) if wkt.starts_with("LINESTRING(") => {
                let inner = &wkt[11..wkt.len() - 1];
                let points: Vec<&str> = inner.split(',').collect();
                if points.len() >= 4 {
                    let first = points.first().map(|s| s.trim());
                    let last = points.last().map(|s| s.trim());
                    Ok(Value::Bool(first == last))
                } else {
                    Ok(Value::Bool(false))
                }
            }
            Value::Geography(_) => Ok(Value::Bool(false)),
            _ => Err(Error::InvalidQuery(
                "ST_ISRING expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_maxdistance(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_MAXDISTANCE requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let points1 = Self::extract_points(&geom1);
                let points2 = Self::extract_points(&geom2);
                let mut max_distance = 0.0_f64;
                for p1 in &points1 {
                    for p2 in &points2 {
                        let dist = p1.geodesic_distance(p2);
                        if dist > max_distance {
                            max_distance = dist;
                        }
                    }
                }
                Ok(Value::Float64(OrderedFloat(max_distance)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_MAXDISTANCE expects geography arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_st_geohash(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        let max_len = if args.len() > 1 {
            match &args[1] {
                Value::Int64(i) => *i as usize,
                _ => 12,
            }
        } else {
            12
        };
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) if wkt.starts_with("POINT(") => {
                let inner = &wkt[6..wkt.len() - 1];
                let parts: Vec<&str> = inner.split_whitespace().collect();
                if parts.len() >= 2 {
                    let lon: f64 = parts[0].parse().unwrap_or(0.0);
                    let lat: f64 = parts[1].parse().unwrap_or(0.0);
                    let hash = geohash::encode(geohash::Coord { x: lon, y: lat }, max_len)
                        .unwrap_or_else(|_| "".to_string());
                    Ok(Value::String(hash))
                } else {
                    Ok(Value::Null)
                }
            }
            Value::Geography(_) => Ok(Value::Null),
            _ => Err(Error::InvalidQuery(
                "ST_GEOHASH expects a geography argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_geogpointfromgeohash(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(hash) => match geohash::decode(hash) {
                Ok((coord, _, _)) => {
                    Ok(Value::Geography(format!("POINT({} {})", coord.x, coord.y)))
                }
                Err(_) => Ok(Value::Null),
            },
            _ => Err(Error::InvalidQuery(
                "ST_GEOGPOINTFROMGEOHASH expects a string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_st_bufferwithtolerance(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Ok(Value::Null);
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt), Value::Float64(_))
            | (Value::Geography(wkt), Value::Int64(_)) => {
                let distance_meters = match &args[1] {
                    Value::Float64(f) => f.0,
                    Value::Int64(i) => *i as f64,
                    _ => 0.0,
                };
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let buffered = Self::create_buffer(&geom, distance_meters);
                Ok(Value::Geography(Self::geometry_to_wkt(&buffered)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_BUFFERWITHTOLERANCE expects a geography argument".into(),
            )),
        }
    }
}
