#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::collapsible_if)]

mod functions;
mod helpers;

use std::collections::HashMap;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use geo::{
    BooleanOps, BoundingRect, Centroid, Contains, ConvexHull, GeodesicArea, GeodesicDistance,
    GeodesicLength, Intersects, SimplifyVw,
};
use geo_types::{Coord, Geometry, LineString, MultiPolygon, Point, Polygon};
use helpers::{
    add_interval_to_date, add_interval_to_datetime, bucket_date, bucket_datetime,
    date_diff_by_part, extract_datetime_field, extract_json_path, format_date_with_pattern,
    format_datetime_with_pattern, format_time_with_pattern, interval_from_field,
    like_pattern_to_regex, negate_interval, parse_date_string, parse_date_with_pattern,
    parse_datetime_string, parse_datetime_with_pattern, parse_interval_string, parse_range_string,
    parse_time_string, parse_time_with_pattern, parse_timestamp_string, trunc_date, trunc_datetime,
    trunc_time, value_to_json,
};
use ordered_float::OrderedFloat;
use rust_decimal::prelude::ToPrimitive;
use wkt::TryFromWkt;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, IntervalValue, RangeValue, Value};
use yachtsql_ir::{
    AggregateFunction, BinaryOp, DateTimeField, Expr, FunctionArg, FunctionBody, Literal,
    ScalarFunction, TrimWhere, UnaryOp, WeekStartDay, WhenClause,
};
use yachtsql_storage::{Record, Schema};

pub struct UserFunctionDef {
    pub parameters: Vec<FunctionArg>,
    pub body: FunctionBody,
}

pub struct IrEvaluator<'a> {
    schema: &'a Schema,
    variables: Option<&'a HashMap<String, Value>>,
    system_variables: Option<&'a HashMap<String, Value>>,
    user_functions: Option<&'a HashMap<String, UserFunctionDef>>,
}

impl<'a> IrEvaluator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            variables: None,
            system_variables: None,
            user_functions: None,
        }
    }

    pub fn with_variables(mut self, variables: &'a HashMap<String, Value>) -> Self {
        self.variables = Some(variables);
        self
    }

    pub fn with_system_variables(mut self, system_variables: &'a HashMap<String, Value>) -> Self {
        self.system_variables = Some(system_variables);
        self
    }

    pub fn with_user_functions(
        mut self,
        user_functions: &'a HashMap<String, UserFunctionDef>,
    ) -> Self {
        self.user_functions = Some(user_functions);
        self
    }

    pub fn eval_scalar_function_with_values(
        &self,
        func: &ScalarFunction,
        arg_values: &[Value],
    ) -> Result<Value> {
        let vec_values: Vec<Value> = arg_values.to_vec();
        match func {
            ScalarFunction::Upper => self.fn_upper(&vec_values),
            ScalarFunction::Lower => self.fn_lower(&vec_values),
            ScalarFunction::Length => self.fn_length(&vec_values),
            ScalarFunction::Coalesce => self.fn_coalesce(&vec_values),
            ScalarFunction::IfNull | ScalarFunction::Ifnull => self.fn_ifnull(&vec_values),
            ScalarFunction::NullIf => self.fn_nullif(&vec_values),
            ScalarFunction::If => self.fn_if(&vec_values),
            ScalarFunction::Abs => self.fn_abs(&vec_values),
            ScalarFunction::Floor => self.fn_floor(&vec_values),
            ScalarFunction::Ceil => self.fn_ceil(&vec_values),
            ScalarFunction::Round => self.fn_round(&vec_values),
            ScalarFunction::Concat => self.fn_concat(&vec_values),
            ScalarFunction::Greatest => self.fn_greatest(&vec_values),
            ScalarFunction::Least => self.fn_least(&vec_values),
            _ => Err(Error::unsupported(format!(
                "Scalar function {:?} not yet supported in eval_scalar_function_with_values",
                func
            ))),
        }
    }

    pub fn cast_value(val: Value, target_type: &DataType, safe: bool) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::Null);
        }
        let result = match (&val, target_type) {
            (v, DataType::Unknown) => Ok(v.clone()),
            (Value::Int64(n), DataType::String) => Ok(Value::String(n.to_string())),
            (Value::Float64(f), DataType::String) => Ok(Value::String(f.0.to_string())),
            (Value::Bool(b), DataType::String) => Ok(Value::String(b.to_string())),
            (Value::String(s), DataType::Int64) => s
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|_| Error::InvalidQuery(format!("Cannot cast '{}' to INT64", s))),
            (Value::Float64(f), DataType::Int64) => {
                if f.0.is_nan()
                    || f.0.is_infinite()
                    || f.0 > i64::MAX as f64
                    || f.0 < i64::MIN as f64
                {
                    Err(Error::InvalidQuery(format!(
                        "Cannot cast {} to INT64: value out of range",
                        f.0
                    )))
                } else {
                    Ok(Value::Int64(f.0 as i64))
                }
            }
            (Value::Int64(n), DataType::Float64) => Ok(Value::Float64(OrderedFloat(*n as f64))),
            (Value::String(s), DataType::Float64) => s
                .parse::<f64>()
                .map(|f| Value::Float64(OrderedFloat(f)))
                .map_err(|_| Error::InvalidQuery(format!("Cannot cast '{}' to FLOAT64", s))),
            _ => Ok(val.clone()),
        };
        if safe {
            Ok(result.unwrap_or(Value::Null))
        } else {
            result
        }
    }

    pub fn evaluate(&self, expr: &Expr, record: &Record) -> Result<Value> {
        match expr {
            Expr::Literal(lit) => self.eval_literal(lit),
            Expr::Column { table, name, index } => {
                self.eval_column(table.as_deref(), name, *index, record)
            }
            Expr::BinaryOp { left, op, right } => self.eval_binary_op(left, *op, right, record),
            Expr::UnaryOp { op, expr } => self.eval_unary_op(*op, expr, record),
            Expr::ScalarFunction { name, args } => self.eval_scalar_function(name, args, record),
            Expr::Aggregate { .. } => Err(Error::InvalidQuery(
                "Aggregates should be evaluated by aggregate executor".into(),
            )),
            Expr::UserDefinedAggregate { .. } => Err(Error::InvalidQuery(
                "User-defined aggregates should be evaluated by aggregate executor".into(),
            )),
            Expr::Window { .. } => Err(Error::InvalidQuery(
                "Window functions should be evaluated by window executor".into(),
            )),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => self.eval_case(
                operand.as_deref(),
                when_clauses,
                else_result.as_deref(),
                record,
            ),
            Expr::Cast {
                expr,
                data_type,
                safe,
            } => self.eval_cast(expr, data_type, *safe, record),
            Expr::IsNull { expr, negated } => self.eval_is_null(expr, *negated, record),
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => self.eval_is_distinct_from(left, right, *negated, record),
            Expr::InList {
                expr,
                list,
                negated,
            } => self.eval_in_list(expr, list, *negated, record),
            Expr::InSubquery { .. } => Err(Error::InvalidQuery(
                "Subqueries should be evaluated by plan executor".into(),
            )),
            Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => self.eval_in_unnest(expr, array_expr, *negated, record),
            Expr::Exists { .. } => Err(Error::InvalidQuery(
                "EXISTS should be evaluated by plan executor".into(),
            )),
            Expr::ArraySubquery(_) => Err(Error::InvalidQuery(
                "ArraySubquery should be evaluated by plan executor".into(),
            )),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => self.eval_between(expr, low, high, *negated, record),
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => self.eval_like(expr, pattern, *negated, *case_insensitive, record),
            Expr::Extract { field, expr } => self.eval_extract(*field, expr, record),
            Expr::Substring {
                expr,
                start,
                length,
            } => self.eval_substring(expr, start.as_deref(), length.as_deref(), record),
            Expr::Trim {
                expr,
                trim_what,
                trim_where,
            } => self.eval_trim(expr, trim_what.as_deref(), *trim_where, record),
            Expr::Position { substr, string } => self.eval_position(substr, string, record),
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => self.eval_overlay(
                expr,
                overlay_what,
                overlay_from,
                overlay_for.as_deref(),
                record,
            ),
            Expr::Array { elements, .. } => self.eval_array(elements, record),
            Expr::ArrayAccess { array, index } => self.eval_array_access(array, index, record),
            Expr::Struct { fields } => self.eval_struct(fields, record),
            Expr::StructAccess { expr, field } => self.eval_struct_access(expr, field, record),
            Expr::TypedString { data_type, value } => self.eval_typed_string(data_type, value),
            Expr::Interval {
                value,
                leading_field,
            } => self.eval_interval(value, *leading_field, record),
            Expr::Alias { expr, .. } => self.evaluate(expr, record),
            Expr::Wildcard { .. } => Err(Error::InvalidQuery(
                "Wildcard should be expanded before evaluation".into(),
            )),
            Expr::Subquery(_) => Err(Error::InvalidQuery(
                "Subqueries should be evaluated by plan executor".into(),
            )),
            Expr::AggregateWindow { .. } => Err(Error::InvalidQuery(
                "Aggregate window functions should be evaluated by plan executor".into(),
            )),
            Expr::ScalarSubquery(_) => Err(Error::InvalidQuery(
                "Scalar subqueries should be evaluated by plan executor".into(),
            )),
            Expr::Parameter { name } => {
                Err(Error::InvalidQuery(format!("Unbound parameter: {}", name)))
            }
            Expr::Variable { name } => {
                if name.starts_with("@@") {
                    if let Some(sys_vars) = self.system_variables {
                        if let Some(val) = sys_vars.get(name) {
                            return Ok(val.clone());
                        }
                    }
                    return Err(Error::InvalidQuery(format!(
                        "System variable '{}' not found",
                        name
                    )));
                }
                if let Some(vars) = self.variables {
                    let upper_name = name.to_uppercase();
                    if let Some(val) = vars.get(&upper_name) {
                        return Ok(val.clone());
                    }
                }
                Err(Error::InvalidQuery(format!(
                    "Variable '{}' not in scope",
                    name
                )))
            }
            Expr::Placeholder { id } => {
                Err(Error::InvalidQuery(format!("Unbound placeholder: {}", id)))
            }
            Expr::Lambda { .. } => Err(Error::InvalidQuery(
                "Lambda expressions should be evaluated inline".into(),
            )),
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => self.eval_at_time_zone(timestamp, time_zone, record),
            Expr::JsonAccess { expr, path } => self.eval_json_access(expr, path, record),
            Expr::Default => Ok(Value::Default),
        }
    }

    fn eval_at_time_zone(
        &self,
        timestamp: &Expr,
        time_zone: &Expr,
        record: &Record,
    ) -> Result<Value> {
        let ts_val = self.evaluate(timestamp, record)?;
        let tz_val = self.evaluate(time_zone, record)?;
        match (ts_val, tz_val) {
            (Value::Timestamp(ts), Value::String(tz_name)) => Ok(Value::Timestamp(ts)),
            (Value::DateTime(dt), Value::String(_)) => Ok(Value::DateTime(dt)),
            (v, _) => Ok(v),
        }
    }

    fn eval_json_access(
        &self,
        expr: &Expr,
        path: &[yachtsql_ir::JsonPathElement],
        record: &Record,
    ) -> Result<Value> {
        let base = self.evaluate(expr, record)?;
        let mut current = match base {
            Value::Json(j) => j,
            Value::String(s) => {
                serde_json::from_str(&s).map_err(|_| Error::InvalidQuery("Invalid JSON".into()))?
            }
            _ => return Err(Error::InvalidQuery("Cannot access non-JSON value".into())),
        };
        for element in path {
            match element {
                yachtsql_ir::JsonPathElement::Key(key) => {
                    current = current.get(key).cloned().unwrap_or(serde_json::Value::Null);
                }
                yachtsql_ir::JsonPathElement::Index(idx) => {
                    current = current
                        .get(*idx as usize)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                }
            }
        }
        Ok(Value::Json(current))
    }

    fn eval_literal(&self, lit: &Literal) -> Result<Value> {
        match lit {
            Literal::Null => Ok(Value::Null),
            Literal::Bool(b) => Ok(Value::Bool(*b)),
            Literal::Int64(n) => Ok(Value::Int64(*n)),
            Literal::Float64(f) => Ok(Value::Float64(OrderedFloat(f.0))),
            Literal::Numeric(d) => Ok(Value::Numeric(*d)),
            Literal::String(s) => Ok(Value::String(s.clone())),
            Literal::Bytes(b) => Ok(Value::Bytes(b.clone())),
            Literal::Date(days) => {
                let date = NaiveDate::from_num_days_from_ce_opt(*days + 719163)
                    .ok_or_else(|| Error::InvalidQuery("Invalid date".into()))?;
                Ok(Value::Date(date))
            }
            Literal::Time(micros) => {
                let secs = (*micros / 1_000_000) as u32;
                let nanos = ((*micros % 1_000_000) * 1000) as u32;
                let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                    .ok_or_else(|| Error::InvalidQuery("Invalid time".into()))?;
                Ok(Value::Time(time))
            }
            Literal::Timestamp(micros) => {
                let dt = DateTime::from_timestamp_micros(*micros)
                    .ok_or_else(|| Error::InvalidQuery("Invalid timestamp".into()))?;
                Ok(Value::Timestamp(dt))
            }
            Literal::Interval {
                months,
                days,
                nanos,
            } => Ok(Value::Interval(IntervalValue {
                months: *months,
                days: *days,
                nanos: *nanos,
            })),
            Literal::Array(elements) => {
                let values: Result<Vec<_>> =
                    elements.iter().map(|e| self.eval_literal(e)).collect();
                Ok(Value::Array(values?))
            }
            Literal::Struct(fields) => {
                let struct_fields: Result<Vec<_>> = fields
                    .iter()
                    .map(|(name, lit)| {
                        let val = self.eval_literal(lit)?;
                        Ok((name.clone(), val))
                    })
                    .collect();
                Ok(Value::Struct(struct_fields?))
            }
            Literal::Json(json) => Ok(Value::Json(json.clone())),
            Literal::BigNumeric(d) => Ok(Value::BigNumeric(*d)),
            Literal::Datetime(micros) => {
                let secs = *micros / 1_000_000;
                let nanos = ((*micros % 1_000_000) * 1000) as u32;
                let dt = chrono::DateTime::from_timestamp(secs, nanos)
                    .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))?
                    .naive_utc();
                Ok(Value::DateTime(dt))
            }
        }
    }

    fn eval_column(
        &self,
        table: Option<&str>,
        name: &str,
        index: Option<usize>,
        record: &Record,
    ) -> Result<Value> {
        if let Some(idx) = index {
            if idx < record.values().len() {
                return Ok(record.values()[idx].clone());
            }
        }

        if let Some(vars) = self.variables {
            let upper_name = name.to_uppercase();
            if let Some(val) = vars.get(&upper_name) {
                return Ok(val.clone());
            }
            if let Some(table_name) = table {
                let upper_table = table_name.to_uppercase();
                if let Some(Value::Struct(fields)) = vars.get(&upper_table) {
                    let field_name_lower = name.to_lowercase();
                    for (field_name, field_value) in fields {
                        if field_name.to_lowercase() == field_name_lower {
                            return Ok(field_value.clone());
                        }
                    }
                }
            }
        }

        let idx = self.schema.field_index_qualified(name, table);

        match idx {
            Some(i) if i < record.values().len() => Ok(record.values()[i].clone()),
            _ => Err(Error::ColumnNotFound(name.to_string())),
        }
    }

    fn get_collation_for_expr(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column { name, table, .. } => {
                let upper_col = name.to_uppercase();
                for field in self.schema.fields() {
                    if field.name.to_uppercase() == upper_col {
                        if let Some(src) = &field.source_table {
                            if let Some(prefix) = table {
                                if prefix.to_uppercase() != src.to_uppercase() {
                                    continue;
                                }
                            }
                        }
                        return field.collation.clone();
                    }
                }
                None
            }
            _ => None,
        }
    }

    pub fn eval_binary_op_with_values(
        &self,
        op: BinaryOp,
        left_val: Value,
        right_val: Value,
    ) -> Result<Value> {
        match op {
            BinaryOp::Add => self.add_values(&left_val, &right_val),
            BinaryOp::Sub => self.sub_values(&left_val, &right_val),
            BinaryOp::Mul => self.mul_values(&left_val, &right_val),
            BinaryOp::Div => self.div_values(&left_val, &right_val),
            BinaryOp::Mod => self.mod_values(&left_val, &right_val),
            BinaryOp::Eq => self.eq_values(&left_val, &right_val),
            BinaryOp::NotEq => self.neq_values(&left_val, &right_val),
            BinaryOp::Lt => self.compare_values(&left_val, &right_val, |ord| ord.is_lt()),
            BinaryOp::LtEq => self.compare_values(&left_val, &right_val, |ord| ord.is_le()),
            BinaryOp::Gt => self.compare_values(&left_val, &right_val, |ord| ord.is_gt()),
            BinaryOp::GtEq => self.compare_values(&left_val, &right_val, |ord| ord.is_ge()),
            BinaryOp::And => self.and_values(&left_val, &right_val),
            BinaryOp::Or => self.or_values(&left_val, &right_val),
            BinaryOp::Concat => self.concat_values(&left_val, &right_val),
            BinaryOp::BitwiseAnd => self.bitwise_and(&left_val, &right_val),
            BinaryOp::BitwiseOr => self.bitwise_or(&left_val, &right_val),
            BinaryOp::BitwiseXor => self.bitwise_xor(&left_val, &right_val),
            BinaryOp::ShiftLeft => self.shift_left(&left_val, &right_val),
            BinaryOp::ShiftRight => self.shift_right(&left_val, &right_val),
        }
    }

    fn eval_binary_op(
        &self,
        left: &Expr,
        op: BinaryOp,
        right: &Expr,
        record: &Record,
    ) -> Result<Value> {
        let left_val = self.evaluate(left, record)?;
        let right_val = self.evaluate(right, record)?;

        match op {
            BinaryOp::Add => self.add_values(&left_val, &right_val),
            BinaryOp::Sub => self.sub_values(&left_val, &right_val),
            BinaryOp::Mul => self.mul_values(&left_val, &right_val),
            BinaryOp::Div => self.div_values(&left_val, &right_val),
            BinaryOp::Mod => self.mod_values(&left_val, &right_val),
            BinaryOp::Eq => {
                let collation = self
                    .get_collation_for_expr(left)
                    .or_else(|| self.get_collation_for_expr(right));
                self.eq_values_with_collation(&left_val, &right_val, collation.as_deref())
            }
            BinaryOp::NotEq => {
                let collation = self
                    .get_collation_for_expr(left)
                    .or_else(|| self.get_collation_for_expr(right));
                self.neq_values_with_collation(&left_val, &right_val, collation.as_deref())
            }
            BinaryOp::Lt => self.compare_values(&left_val, &right_val, |ord| ord.is_lt()),
            BinaryOp::LtEq => self.compare_values(&left_val, &right_val, |ord| ord.is_le()),
            BinaryOp::Gt => self.compare_values(&left_val, &right_val, |ord| ord.is_gt()),
            BinaryOp::GtEq => self.compare_values(&left_val, &right_val, |ord| ord.is_ge()),
            BinaryOp::And => self.and_values(&left_val, &right_val),
            BinaryOp::Or => self.or_values(&left_val, &right_val),
            BinaryOp::Concat => self.concat_values(&left_val, &right_val),
            BinaryOp::BitwiseAnd => self.bitwise_and(&left_val, &right_val),
            BinaryOp::BitwiseOr => self.bitwise_or(&left_val, &right_val),
            BinaryOp::BitwiseXor => self.bitwise_xor(&left_val, &right_val),
            BinaryOp::ShiftLeft => self.shift_left(&left_val, &right_val),
            BinaryOp::ShiftRight => self.shift_right(&left_val, &right_val),
        }
    }

    fn eval_unary_op(&self, op: UnaryOp, expr: &Expr, record: &Record) -> Result<Value> {
        let val = self.evaluate(expr, record)?;

        match op {
            UnaryOp::Not => match val {
                Value::Null => Ok(Value::Null),
                Value::Bool(b) => Ok(Value::Bool(!b)),
                _ => Err(Error::InvalidQuery("NOT requires boolean operand".into())),
            },
            UnaryOp::Minus => match val {
                Value::Null => Ok(Value::Null),
                Value::Int64(n) => Ok(Value::Int64(-n)),
                Value::Float64(f) => Ok(Value::Float64(OrderedFloat(-f.0))),
                Value::Numeric(d) => Ok(Value::Numeric(-d)),
                _ => Err(Error::InvalidQuery(
                    "Unary minus requires numeric operand".into(),
                )),
            },
            UnaryOp::Plus => Ok(val),
            UnaryOp::BitwiseNot => match val {
                Value::Null => Ok(Value::Null),
                Value::Int64(n) => Ok(Value::Int64(!n)),
                _ => Err(Error::InvalidQuery(
                    "Bitwise NOT requires integer operand".into(),
                )),
            },
        }
    }

    fn eval_scalar_function(
        &self,
        func: &ScalarFunction,
        args: &[Expr],
        record: &Record,
    ) -> Result<Value> {
        let arg_values: Vec<Value> = args
            .iter()
            .map(|a| self.evaluate(a, record))
            .collect::<Result<_>>()?;

        match func {
            ScalarFunction::Upper => self.fn_upper(&arg_values),
            ScalarFunction::Lower => self.fn_lower(&arg_values),
            ScalarFunction::Length => self.fn_length(&arg_values),
            ScalarFunction::Coalesce => self.fn_coalesce(&arg_values),
            ScalarFunction::IfNull | ScalarFunction::Ifnull => self.fn_ifnull(&arg_values),
            ScalarFunction::NullIf => self.fn_nullif(&arg_values),
            ScalarFunction::If => self.fn_if(&arg_values),
            ScalarFunction::Abs => self.fn_abs(&arg_values),
            ScalarFunction::Floor => self.fn_floor(&arg_values),
            ScalarFunction::Ceil => self.fn_ceil(&arg_values),
            ScalarFunction::Round => self.fn_round(&arg_values),
            ScalarFunction::Sqrt => self.fn_sqrt(&arg_values),
            ScalarFunction::Cbrt => self.fn_cbrt(&arg_values),
            ScalarFunction::Power | ScalarFunction::Pow => self.fn_power(&arg_values),
            ScalarFunction::Mod => self.fn_mod(&arg_values),
            ScalarFunction::Sign => self.fn_sign(&arg_values),
            ScalarFunction::Exp => self.fn_exp(&arg_values),
            ScalarFunction::Ln => self.fn_ln(&arg_values),
            ScalarFunction::Log => self.fn_log(&arg_values),
            ScalarFunction::Log10 => self.fn_log10(&arg_values),
            ScalarFunction::Greatest => self.fn_greatest(&arg_values),
            ScalarFunction::Least => self.fn_least(&arg_values),
            ScalarFunction::Trunc => self.fn_trunc(&arg_values),
            ScalarFunction::Div => self.fn_div(&arg_values),
            ScalarFunction::SafeDivide => self.fn_safe_divide(&arg_values),
            ScalarFunction::IeeeDivide => self.fn_ieee_divide(&arg_values),
            ScalarFunction::SafeMultiply => self.fn_safe_multiply(&arg_values),
            ScalarFunction::SafeAdd => self.fn_safe_add(&arg_values),
            ScalarFunction::SafeSubtract => self.fn_safe_subtract(&arg_values),
            ScalarFunction::SafeNegate => self.fn_safe_negate(&arg_values),
            ScalarFunction::Concat => self.fn_concat(&arg_values),
            ScalarFunction::Trim => self.fn_trim(&arg_values),
            ScalarFunction::LTrim => self.fn_ltrim(&arg_values),
            ScalarFunction::RTrim => self.fn_rtrim(&arg_values),
            ScalarFunction::Substr => self.fn_substr(&arg_values),
            ScalarFunction::Replace => self.fn_replace(&arg_values),
            ScalarFunction::Reverse => self.fn_reverse(&arg_values),
            ScalarFunction::Left => self.fn_left(&arg_values),
            ScalarFunction::Right => self.fn_right(&arg_values),
            ScalarFunction::Repeat => self.fn_repeat(&arg_values),
            ScalarFunction::StartsWith => self.fn_starts_with(&arg_values),
            ScalarFunction::EndsWith => self.fn_ends_with(&arg_values),
            ScalarFunction::Contains => self.fn_contains(&arg_values),
            ScalarFunction::Strpos => self.fn_strpos(&arg_values),
            ScalarFunction::Lpad => self.fn_lpad(&arg_values),
            ScalarFunction::Rpad => self.fn_rpad(&arg_values),
            ScalarFunction::Initcap => self.fn_initcap(&arg_values),
            ScalarFunction::Datetime => self.fn_datetime(&arg_values),
            ScalarFunction::Date => self.fn_date(&arg_values),
            ScalarFunction::Time => self.fn_time(&arg_values),
            ScalarFunction::Timestamp => self.fn_timestamp(&arg_values),
            ScalarFunction::ArrayLength => self.fn_array_length(&arg_values),
            ScalarFunction::ArrayToString => self.fn_array_to_string(&arg_values),
            ScalarFunction::GenerateArray => self.fn_generate_array(&arg_values),
            ScalarFunction::GenerateDateArray => self.fn_generate_date_array(&arg_values),
            ScalarFunction::GenerateTimestampArray => self.fn_generate_timestamp_array(&arg_values),
            ScalarFunction::Md5 => self.fn_md5(&arg_values),
            ScalarFunction::Sha256 => self.fn_sha256(&arg_values),
            ScalarFunction::Sha512 => self.fn_sha512(&arg_values),
            ScalarFunction::FarmFingerprint => self.fn_farm_fingerprint(&arg_values),
            ScalarFunction::GenerateUuid => self.fn_generate_uuid(&arg_values),
            ScalarFunction::CurrentDate => self.fn_current_date(&arg_values),
            ScalarFunction::CurrentTimestamp => self.fn_current_timestamp(&arg_values),
            ScalarFunction::CurrentTime => self.fn_current_time(&arg_values),
            ScalarFunction::CurrentDatetime => self.fn_current_datetime(&arg_values),
            ScalarFunction::Zeroifnull => self.fn_zeroifnull(&arg_values),
            ScalarFunction::String => self.fn_string(&arg_values),
            ScalarFunction::RegexpContains => self.fn_regexp_contains(&arg_values),
            ScalarFunction::RegexpReplace => self.fn_regexp_replace(&arg_values),
            ScalarFunction::DateAdd => self.fn_date_add(&arg_values),
            ScalarFunction::DateSub => self.fn_date_sub(&arg_values),
            ScalarFunction::DateDiff => self.fn_date_diff(&arg_values),
            ScalarFunction::DateTrunc => self.fn_date_trunc(&arg_values),
            ScalarFunction::DateBucket => self.fn_date_bucket(&arg_values),
            ScalarFunction::DatetimeTrunc => self.fn_datetime_trunc(&arg_values),
            ScalarFunction::TimestampTrunc => self.fn_timestamp_trunc(&arg_values),
            ScalarFunction::TimeTrunc => self.fn_time_trunc(&arg_values),
            ScalarFunction::DateFromUnixDate => self.fn_date_from_unix_date(&arg_values),
            ScalarFunction::UnixDate => self.fn_unix_date(&arg_values),
            ScalarFunction::UnixMicros => self.fn_unix_micros(&arg_values),
            ScalarFunction::UnixMillis => self.fn_unix_millis(&arg_values),
            ScalarFunction::UnixSeconds => self.fn_unix_seconds(&arg_values),
            ScalarFunction::TimestampMicros => self.fn_timestamp_micros(&arg_values),
            ScalarFunction::TimestampMillis => self.fn_timestamp_millis(&arg_values),
            ScalarFunction::TimestampSeconds => self.fn_timestamp_seconds(&arg_values),
            ScalarFunction::FormatDate => self.fn_format_date(&arg_values),
            ScalarFunction::FormatDatetime => self.fn_format_datetime(&arg_values),
            ScalarFunction::FormatTimestamp => self.fn_format_timestamp(&arg_values),
            ScalarFunction::FormatTime => self.fn_format_time(&arg_values),
            ScalarFunction::ParseDate => self.fn_parse_date(&arg_values),
            ScalarFunction::ParseDatetime => self.fn_parse_datetime(&arg_values),
            ScalarFunction::ParseTimestamp => self.fn_parse_timestamp(&arg_values),
            ScalarFunction::ParseTime => self.fn_parse_time(&arg_values),
            ScalarFunction::LastDay => self.fn_last_day(&arg_values),
            ScalarFunction::DatetimeBucket => self.fn_datetime_bucket(&arg_values),
            ScalarFunction::TimestampBucket => self.fn_timestamp_bucket(&arg_values),
            ScalarFunction::Extract => self.fn_extract_from_args(args, record),
            ScalarFunction::Sin => self.fn_sin(&arg_values),
            ScalarFunction::Cos => self.fn_cos(&arg_values),
            ScalarFunction::Tan => self.fn_tan(&arg_values),
            ScalarFunction::Asin => self.fn_asin(&arg_values),
            ScalarFunction::Acos => self.fn_acos(&arg_values),
            ScalarFunction::Atan => self.fn_atan(&arg_values),
            ScalarFunction::Atan2 => self.fn_atan2(&arg_values),
            ScalarFunction::Sinh => self.fn_sinh(&arg_values),
            ScalarFunction::Cosh => self.fn_cosh(&arg_values),
            ScalarFunction::Tanh => self.fn_tanh(&arg_values),
            ScalarFunction::Asinh => self.fn_asinh(&arg_values),
            ScalarFunction::Acosh => self.fn_acosh(&arg_values),
            ScalarFunction::Atanh => self.fn_atanh(&arg_values),
            ScalarFunction::Cot => self.fn_cot(&arg_values),
            ScalarFunction::Csc => self.fn_csc(&arg_values),
            ScalarFunction::Sec => self.fn_sec(&arg_values),
            ScalarFunction::Coth => self.fn_coth(&arg_values),
            ScalarFunction::Csch => self.fn_csch(&arg_values),
            ScalarFunction::Sech => self.fn_sech(&arg_values),
            ScalarFunction::Pi => Ok(Value::Float64(OrderedFloat(std::f64::consts::PI))),
            ScalarFunction::IsNan => self.fn_is_nan(&arg_values),
            ScalarFunction::IsInf => self.fn_is_inf(&arg_values),
            ScalarFunction::CosineDistance => self.fn_cosine_distance(&arg_values),
            ScalarFunction::EuclideanDistance => self.fn_euclidean_distance(&arg_values),
            ScalarFunction::RangeBucket => self.fn_range_bucket(&arg_values),
            ScalarFunction::Rand | ScalarFunction::RandCanonical => self.fn_rand(&arg_values),
            ScalarFunction::Split => self.fn_split(&arg_values),
            ScalarFunction::ArrayConcat => self.fn_array_concat(&arg_values),
            ScalarFunction::ArrayReverse => self.fn_array_reverse(&arg_values),
            ScalarFunction::Instr => self.fn_instr(&arg_values),
            ScalarFunction::Format => self.fn_format(&arg_values),
            ScalarFunction::Cast | ScalarFunction::SafeCast => self.fn_cast(&arg_values),
            ScalarFunction::RegexpExtract => self.fn_regexp_extract(&arg_values),
            ScalarFunction::RegexpExtractAll => self.fn_regexp_extract_all(&arg_values),
            ScalarFunction::ByteLength | ScalarFunction::CharLength => {
                self.fn_byte_length(&arg_values)
            }
            ScalarFunction::Ascii => self.fn_ascii(&arg_values),
            ScalarFunction::Chr => self.fn_chr(&arg_values),
            ScalarFunction::Unicode => self.fn_unicode(&arg_values),
            ScalarFunction::ToCodePoints => self.fn_to_code_points(&arg_values),
            ScalarFunction::CodePointsToString => self.fn_code_points_to_string(&arg_values),
            ScalarFunction::CodePointsToBytes => self.fn_code_points_to_bytes(&arg_values),
            ScalarFunction::Translate => self.fn_translate(&arg_values),
            ScalarFunction::Soundex => self.fn_soundex(&arg_values),
            ScalarFunction::Normalize => self.fn_normalize(&arg_values),
            ScalarFunction::NormalizeAndCasefold => self.fn_normalize_and_casefold(&arg_values),
            ScalarFunction::EditDistance => self.fn_edit_distance(&arg_values),
            ScalarFunction::ContainsSubstr => self.fn_contains_substr(&arg_values),
            ScalarFunction::ToBase64 => self.fn_to_base64(&arg_values),
            ScalarFunction::FromBase64 => self.fn_from_base64(&arg_values),
            ScalarFunction::ToBase32 => self.fn_to_base32(&arg_values),
            ScalarFunction::FromBase32 => self.fn_from_base32(&arg_values),
            ScalarFunction::ToHex => self.fn_to_hex(&arg_values),
            ScalarFunction::FromHex => self.fn_from_hex(&arg_values),
            ScalarFunction::ToJson => self.fn_to_json(&arg_values),
            ScalarFunction::ToJsonString => self.fn_to_json_string(&arg_values),
            ScalarFunction::ParseJson => self.fn_parse_json(&arg_values),
            ScalarFunction::JsonValue => self.fn_json_value(&arg_values),
            ScalarFunction::JsonQuery => self.fn_json_query(&arg_values),
            ScalarFunction::JsonExtract | ScalarFunction::JsonExtractScalar => {
                self.fn_json_extract(&arg_values)
            }
            ScalarFunction::JsonExtractArray => self.fn_json_extract_array(&arg_values),
            ScalarFunction::TypeOf => self.fn_typeof(&arg_values),
            ScalarFunction::Nvl => self.fn_ifnull(&arg_values),
            ScalarFunction::Nvl2 => self.fn_nvl2(&arg_values),
            ScalarFunction::SafeConvertBytesToString => {
                self.fn_safe_convert_bytes_to_string(&arg_values)
            }
            ScalarFunction::ConvertBytesToString => self.fn_convert_bytes_to_string(&arg_values),
            ScalarFunction::Range => self.fn_range(&arg_values),
            ScalarFunction::JsonType => self.fn_json_type(&arg_values),
            ScalarFunction::BitCount => self.fn_bit_count(&arg_values),
            ScalarFunction::Int64FromJson => self.fn_int64_from_json(&arg_values),
            ScalarFunction::Float64FromJson => self.fn_float64_from_json(&arg_values),
            ScalarFunction::BoolFromJson => self.fn_bool_from_json(&arg_values),
            ScalarFunction::StringFromJson => self.fn_string_from_json(&arg_values),
            ScalarFunction::MakeInterval => self.fn_make_interval(&arg_values),
            ScalarFunction::JustifyDays => self.fn_justify_days(&arg_values),
            ScalarFunction::JustifyHours => self.fn_justify_hours(&arg_values),
            ScalarFunction::JustifyInterval => self.fn_justify_interval(&arg_values),
            ScalarFunction::SafeOffset | ScalarFunction::SafeOrdinal => {
                self.fn_safe_array_access(&arg_values, matches!(func, ScalarFunction::SafeOrdinal))
            }
            ScalarFunction::Custom(name) => self.eval_custom_function(name, &arg_values),
            _ => Err(Error::UnsupportedFeature(format!(
                "Scalar function {:?} not yet implemented in IR evaluator",
                func
            ))),
        }
    }

    fn eval_case(
        &self,
        operand: Option<&Expr>,
        when_clauses: &[WhenClause],
        else_result: Option<&Expr>,
        record: &Record,
    ) -> Result<Value> {
        match operand {
            Some(op_expr) => {
                let op_val = self.evaluate(op_expr, record)?;
                for clause in when_clauses {
                    let when_val = self.evaluate(&clause.condition, record)?;
                    if self.values_equal(&op_val, &when_val) {
                        return self.evaluate(&clause.result, record);
                    }
                }
            }
            None => {
                for clause in when_clauses {
                    let cond_val = self.evaluate(&clause.condition, record)?;
                    if cond_val.as_bool().unwrap_or(false) {
                        return self.evaluate(&clause.result, record);
                    }
                }
            }
        }

        match else_result {
            Some(e) => self.evaluate(e, record),
            None => Ok(Value::Null),
        }
    }

    fn eval_cast(
        &self,
        expr: &Expr,
        target_type: &DataType,
        safe: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(Value::Null);
        }
        let result = match (val, target_type) {
            (v, DataType::Unknown) => Ok(v),

            (Value::Int64(n), DataType::String) => Ok(Value::String(n.to_string())),
            (Value::Float64(f), DataType::String) => Ok(Value::String(f.0.to_string())),
            (Value::Bool(b), DataType::String) => Ok(Value::String(b.to_string())),
            (Value::Date(d), DataType::String) => Ok(Value::String(d.to_string())),
            (Value::Time(t), DataType::String) => Ok(Value::String(t.to_string())),
            (Value::DateTime(dt), DataType::String) => Ok(Value::String(dt.to_string())),
            (Value::Timestamp(ts), DataType::String) => Ok(Value::String(ts.to_rfc3339())),
            (Value::Numeric(n), DataType::String) => Ok(Value::String(n.to_string())),

            (Value::String(s), DataType::Int64) => s
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|_| Error::InvalidQuery(format!("Cannot cast '{}' to INT64", s))),
            (Value::Float64(f), DataType::Int64) => {
                if f.0.is_nan()
                    || f.0.is_infinite()
                    || f.0 > i64::MAX as f64
                    || f.0 < i64::MIN as f64
                {
                    Err(Error::InvalidQuery(format!(
                        "Cannot cast {} to INT64: value out of range",
                        f.0
                    )))
                } else {
                    Ok(Value::Int64(f.0 as i64))
                }
            }
            (Value::Bool(b), DataType::Int64) => Ok(Value::Int64(if b { 1 } else { 0 })),
            (Value::Numeric(n), DataType::Int64) => n
                .to_i64()
                .map(Value::Int64)
                .ok_or_else(|| Error::InvalidQuery("Cannot cast NUMERIC to INT64".into())),

            (Value::String(s), DataType::Float64) => s
                .parse::<f64>()
                .map(|f| Value::Float64(OrderedFloat(f)))
                .map_err(|_| Error::InvalidQuery(format!("Cannot cast '{}' to FLOAT64", s))),
            (Value::Int64(n), DataType::Float64) => Ok(Value::Float64(OrderedFloat(n as f64))),
            (Value::Numeric(n), DataType::Float64) => n
                .to_f64()
                .map(|f| Value::Float64(OrderedFloat(f)))
                .ok_or_else(|| Error::InvalidQuery("Cannot cast NUMERIC to FLOAT64".into())),

            (Value::String(s), DataType::Bool) => match s.to_lowercase().as_str() {
                "true" | "1" => Ok(Value::Bool(true)),
                "false" | "0" => Ok(Value::Bool(false)),
                _ => Err(Error::InvalidQuery(format!("Cannot cast '{}' to BOOL", s))),
            },
            (Value::Int64(n), DataType::Bool) => Ok(Value::Bool(n != 0)),

            (Value::String(s), DataType::Date) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                .map(Value::Date)
                .map_err(|e| Error::InvalidQuery(format!("Cannot cast to DATE: {}", e))),
            (Value::DateTime(dt), DataType::Date) => Ok(Value::Date(dt.date())),
            (Value::Timestamp(ts), DataType::Date) => Ok(Value::Date(ts.date_naive())),

            (Value::String(s), DataType::Time) => NaiveTime::parse_from_str(&s, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(&s, "%H:%M:%S%.f"))
                .map(Value::Time)
                .map_err(|e| Error::InvalidQuery(format!("Cannot cast to TIME: {}", e))),
            (Value::DateTime(dt), DataType::Time) => Ok(Value::Time(dt.time())),
            (Value::Timestamp(ts), DataType::Time) => Ok(Value::Time(ts.time())),

            (Value::String(s), DataType::DateTime) => {
                NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f"))
                    .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S"))
                    .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f"))
                    .map(Value::DateTime)
                    .map_err(|e| Error::InvalidQuery(format!("Cannot cast to DATETIME: {}", e)))
            }
            (Value::Date(d), DataType::DateTime) => d
                .and_hms_opt(0, 0, 0)
                .map(Value::DateTime)
                .ok_or_else(|| Error::InvalidQuery("Cannot convert DATE to DATETIME".into())),
            (Value::Timestamp(ts), DataType::DateTime) => Ok(Value::DateTime(ts.naive_utc())),

            (Value::String(s), DataType::Timestamp) => DateTime::parse_from_rfc3339(&s)
                .map(|d| Value::Timestamp(d.with_timezone(&Utc)))
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f"))
                        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S"))
                        .map(|ndt| Value::Timestamp(ndt.and_utc()))
                })
                .map_err(|e| Error::InvalidQuery(format!("Cannot cast to TIMESTAMP: {}", e))),
            (Value::Date(d), DataType::Timestamp) => d
                .and_hms_opt(0, 0, 0)
                .map(|dt| Value::Timestamp(dt.and_utc()))
                .ok_or_else(|| Error::InvalidQuery("Cannot convert DATE to TIMESTAMP".into())),
            (Value::DateTime(dt), DataType::Timestamp) => Ok(Value::Timestamp(dt.and_utc())),

            (Value::Int64(n), DataType::Numeric(_) | DataType::BigNumeric) => {
                Ok(Value::Numeric(rust_decimal::Decimal::from(n)))
            }
            (Value::Float64(f), DataType::Numeric(_) | DataType::BigNumeric) => {
                rust_decimal::Decimal::try_from(f.0)
                    .map(Value::Numeric)
                    .map_err(|e| Error::InvalidQuery(format!("Cannot cast to NUMERIC: {}", e)))
            }
            (Value::String(s), DataType::Numeric(_) | DataType::BigNumeric) => s
                .parse::<rust_decimal::Decimal>()
                .map(Value::Numeric)
                .map_err(|e| Error::InvalidQuery(format!("Cannot cast to NUMERIC: {}", e))),

            (Value::String(s), DataType::Bytes) => Ok(Value::Bytes(s.as_bytes().to_vec())),
            (Value::Bytes(b), DataType::String) => String::from_utf8(b)
                .map(Value::String)
                .map_err(|e| Error::InvalidQuery(format!("Cannot cast BYTES to STRING: {}", e))),

            (Value::Array(arr), DataType::Array(elem_type)) => {
                let mut new_arr = Vec::with_capacity(arr.len());
                for elem in arr {
                    let casted = Self::cast_value(elem, elem_type, safe)?;
                    new_arr.push(casted);
                }
                Ok(Value::Array(new_arr))
            }

            (v, _) if v.data_type() == *target_type => Ok(v),

            (v, t) => Err(Error::InvalidQuery(format!(
                "Cannot cast {:?} to {:?}",
                v.data_type(),
                t
            ))),
        };

        if safe {
            Ok(result.unwrap_or(Value::Null))
        } else {
            result
        }
    }

    fn eval_is_null(&self, expr: &Expr, negated: bool, record: &Record) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let is_null = val.is_null();
        Ok(Value::Bool(if negated { !is_null } else { is_null }))
    }

    fn eval_is_distinct_from(
        &self,
        left: &Expr,
        right: &Expr,
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let left_val = self.evaluate(left, record)?;
        let right_val = self.evaluate(right, record)?;

        let both_null = left_val.is_null() && right_val.is_null();
        let either_null = left_val.is_null() || right_val.is_null();

        let is_distinct = if both_null {
            false
        } else if either_null {
            true
        } else {
            !self.values_equal(&left_val, &right_val)
        };

        Ok(Value::Bool(if negated {
            !is_distinct
        } else {
            is_distinct
        }))
    }

    fn eval_in_list(
        &self,
        expr: &Expr,
        list: &[Expr],
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(Value::Null);
        }

        let mut has_null = false;
        for item in list {
            let item_val = self.evaluate(item, record)?;
            if item_val.is_null() {
                has_null = true;
            } else if self.values_equal(&val, &item_val) {
                return Ok(Value::Bool(!negated));
            }
        }

        if has_null {
            Ok(Value::Null)
        } else {
            Ok(Value::Bool(negated))
        }
    }

    fn eval_in_unnest(
        &self,
        expr: &Expr,
        array_expr: &Expr,
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let array_val = self.evaluate(array_expr, record)?;

        match array_val {
            Value::Array(elements) => {
                if val.is_null() {
                    return Ok(Value::Null);
                }
                let mut has_null = false;
                for elem in &elements {
                    if elem.is_null() {
                        has_null = true;
                    } else if self.values_equal(&val, elem) {
                        return Ok(Value::Bool(!negated));
                    }
                }
                if has_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(negated))
                }
            }
            Value::Null => Ok(Value::Null),
            _ => Err(Error::InvalidQuery(
                "IN UNNEST requires array argument".into(),
            )),
        }
    }

    fn eval_between(
        &self,
        expr: &Expr,
        low: &Expr,
        high: &Expr,
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let low_val = self.evaluate(low, record)?;
        let high_val = self.evaluate(high, record)?;

        if val.is_null() || low_val.is_null() || high_val.is_null() {
            return Ok(Value::Null);
        }

        let ge_low = self.compare_values(&val, &low_val, |ord| ord.is_ge())?;
        let le_high = self.compare_values(&val, &high_val, |ord| ord.is_le())?;

        match (ge_low, le_high) {
            (Value::Bool(g), Value::Bool(l)) => {
                Ok(Value::Bool(if negated { !(g && l) } else { g && l }))
            }
            _ => Ok(Value::Null),
        }
    }

    fn eval_like(
        &self,
        expr: &Expr,
        pattern: &Expr,
        negated: bool,
        case_insensitive: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let pat = self.evaluate(pattern, record)?;

        match (&val, &pat) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(p)) => {
                let regex_pattern = like_pattern_to_regex(p);
                let regex = if case_insensitive {
                    regex::Regex::new(&format!("(?i)^{}$", regex_pattern))
                } else {
                    regex::Regex::new(&format!("^{}$", regex_pattern))
                }
                .map_err(|e| Error::InvalidQuery(format!("Invalid LIKE pattern: {}", e)))?;
                let matches = regex.is_match(s);
                Ok(Value::Bool(if negated { !matches } else { matches }))
            }
            _ => Err(Error::InvalidQuery("LIKE requires string arguments".into())),
        }
    }

    fn eval_extract(&self, field: DateTimeField, expr: &Expr, record: &Record) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        extract_datetime_field(&val, field)
    }

    fn eval_substring(
        &self,
        expr: &Expr,
        start: Option<&Expr>,
        length: Option<&Expr>,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let start_val = start.map(|e| self.evaluate(e, record)).transpose()?;
        let len_val = length.map(|e| self.evaluate(e, record)).transpose()?;

        match val {
            Value::Null => Ok(Value::Null),
            Value::String(s) => {
                let chars: Vec<char> = s.chars().collect();
                let char_len = chars.len();
                let start_raw = start_val.and_then(|v| v.as_i64()).unwrap_or(1);

                let start_idx = if start_raw < 0 {
                    char_len.saturating_sub((-start_raw) as usize)
                } else if start_raw == 0 {
                    0
                } else {
                    (start_raw as usize).saturating_sub(1).min(char_len)
                };

                let len = len_val
                    .and_then(|v| v.as_i64())
                    .map(|n| n.max(0) as usize)
                    .unwrap_or(char_len.saturating_sub(start_idx));
                let result: String = chars.into_iter().skip(start_idx).take(len).collect();
                Ok(Value::String(result))
            }
            Value::Bytes(b) => {
                let byte_len = b.len();
                let start_raw = start_val.and_then(|v| v.as_i64()).unwrap_or(1);

                let start_idx = if start_raw < 0 {
                    byte_len.saturating_sub((-start_raw) as usize)
                } else if start_raw == 0 {
                    0
                } else {
                    (start_raw as usize).saturating_sub(1).min(byte_len)
                };

                let len = len_val
                    .and_then(|v| v.as_i64())
                    .map(|n| n.max(0) as usize)
                    .unwrap_or(byte_len.saturating_sub(start_idx));
                let end_idx = (start_idx + len).min(byte_len);
                let result: Vec<u8> = b[start_idx..end_idx].to_vec();
                Ok(Value::Bytes(result))
            }
            _ => Err(Error::InvalidQuery(
                "SUBSTRING requires string or bytes argument".into(),
            )),
        }
    }

    fn eval_trim(
        &self,
        expr: &Expr,
        trim_what: Option<&Expr>,
        trim_where: TrimWhere,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let trim_chars = trim_what
            .map(|e| self.evaluate(e, record))
            .transpose()?
            .and_then(|v| v.as_str().map(|s| s.to_string()));

        match val {
            Value::Null => Ok(Value::Null),
            Value::String(s) => {
                let chars: Vec<char> = trim_chars
                    .as_ref()
                    .map(|t| t.chars().collect())
                    .unwrap_or_else(|| vec![' ']);
                let result = match trim_where {
                    TrimWhere::Both => s.trim_matches(|c| chars.contains(&c)).to_string(),
                    TrimWhere::Leading => s.trim_start_matches(|c| chars.contains(&c)).to_string(),
                    TrimWhere::Trailing => s.trim_end_matches(|c| chars.contains(&c)).to_string(),
                };
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery("TRIM requires string argument".into())),
        }
    }

    fn eval_position(&self, substr: &Expr, string: &Expr, record: &Record) -> Result<Value> {
        let substr_val = self.evaluate(substr, record)?;
        let string_val = self.evaluate(string, record)?;

        match (&substr_val, &string_val) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(sub), Value::String(s)) => {
                let pos = s.find(sub).map(|i| i + 1).unwrap_or(0);
                Ok(Value::Int64(pos as i64))
            }
            _ => Err(Error::InvalidQuery(
                "POSITION requires string arguments".into(),
            )),
        }
    }

    fn eval_overlay(
        &self,
        expr: &Expr,
        overlay_what: &Expr,
        overlay_from: &Expr,
        overlay_for: Option<&Expr>,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let replacement = self.evaluate(overlay_what, record)?;
        let start = self.evaluate(overlay_from, record)?;
        let len = overlay_for.map(|e| self.evaluate(e, record)).transpose()?;

        match (&val, &replacement, &start) {
            (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(r), Value::Int64(start_pos)) => {
                let start_idx = (*start_pos - 1).max(0) as usize;
                let replace_len = len
                    .and_then(|v| v.as_i64())
                    .map(|n| n.max(0) as usize)
                    .unwrap_or(r.len());
                let chars: Vec<char> = s.chars().collect();
                let mut result = String::new();
                result.extend(chars.iter().take(start_idx));
                result.push_str(r);
                result.extend(chars.iter().skip(start_idx + replace_len));
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery(
                "OVERLAY requires string arguments".into(),
            )),
        }
    }

    fn eval_array(&self, elements: &[Expr], record: &Record) -> Result<Value> {
        let values: Result<Vec<_>> = elements.iter().map(|e| self.evaluate(e, record)).collect();
        Ok(Value::Array(values?))
    }

    fn eval_array_access(&self, array: &Expr, index: &Expr, record: &Record) -> Result<Value> {
        let array_val = self.evaluate(array, record)?;

        match index {
            Expr::ScalarFunction {
                name: ScalarFunction::SafeOffset,
                args,
            } if args.len() == 1 => {
                let idx_val = self.evaluate(&args[0], record)?;
                match (&array_val, &idx_val) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Array(elements), Value::Int64(idx)) => {
                        if *idx < 0 || *idx as usize >= elements.len() {
                            Ok(Value::Null)
                        } else {
                            Ok(elements[*idx as usize].clone())
                        }
                    }
                    _ => Err(Error::InvalidQuery(
                        "Array access requires array and integer index".into(),
                    )),
                }
            }
            Expr::ScalarFunction {
                name: ScalarFunction::SafeOrdinal,
                args,
            } if args.len() == 1 => {
                let idx_val = self.evaluate(&args[0], record)?;
                match (&array_val, &idx_val) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Array(elements), Value::Int64(idx)) => {
                        let zero_based = idx - 1;
                        if zero_based < 0 || zero_based as usize >= elements.len() {
                            Ok(Value::Null)
                        } else {
                            Ok(elements[zero_based as usize].clone())
                        }
                    }
                    _ => Err(Error::InvalidQuery(
                        "Array access requires array and integer index".into(),
                    )),
                }
            }
            Expr::ScalarFunction {
                name: ScalarFunction::ArrayOffset,
                args,
            } if args.len() == 1 => {
                let idx_val = self.evaluate(&args[0], record)?;
                match (&array_val, &idx_val) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Array(elements), Value::Int64(idx)) => {
                        if *idx < 0 || *idx as usize >= elements.len() {
                            Err(Error::InvalidQuery(format!(
                                "Array index {} out of bounds for array of length {}",
                                idx,
                                elements.len()
                            )))
                        } else {
                            Ok(elements[*idx as usize].clone())
                        }
                    }
                    _ => Err(Error::InvalidQuery(
                        "Array access requires array and integer index".into(),
                    )),
                }
            }
            Expr::ScalarFunction {
                name: ScalarFunction::ArrayOrdinal,
                args,
            } if args.len() == 1 => {
                let idx_val = self.evaluate(&args[0], record)?;
                match (&array_val, &idx_val) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Array(elements), Value::Int64(idx)) => {
                        let zero_based = idx - 1;
                        if zero_based < 0 || zero_based as usize >= elements.len() {
                            Err(Error::InvalidQuery(format!(
                                "Array index {} out of bounds for array of length {}",
                                idx,
                                elements.len()
                            )))
                        } else {
                            Ok(elements[zero_based as usize].clone())
                        }
                    }
                    _ => Err(Error::InvalidQuery(
                        "Array access requires array and integer index".into(),
                    )),
                }
            }
            _ => {
                let index_val = self.evaluate(index, record)?;
                match (&array_val, &index_val) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Array(elements), Value::Int64(idx)) => {
                        let actual_idx = if *idx < 0 {
                            (elements.len() as i64 + idx) as usize
                        } else if *idx == 0 {
                            return Err(Error::InvalidQuery(
                                "Array index 0 is not valid, arrays are 1-indexed".into(),
                            ));
                        } else {
                            (*idx - 1) as usize
                        };
                        Ok(elements.get(actual_idx).cloned().unwrap_or(Value::Null))
                    }
                    (Value::Json(json), Value::Int64(idx)) => {
                        let result = json
                            .get(*idx as usize)
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        Ok(Value::Json(result))
                    }
                    (Value::Json(json), Value::String(key)) => {
                        let result = json.get(key).cloned().unwrap_or(serde_json::Value::Null);
                        Ok(Value::Json(result))
                    }
                    _ => Err(Error::InvalidQuery(
                        "Array access requires array and integer index".into(),
                    )),
                }
            }
        }
    }

    fn eval_struct(&self, fields: &[(Option<String>, Expr)], record: &Record) -> Result<Value> {
        let struct_fields: Result<Vec<_>> = fields
            .iter()
            .enumerate()
            .map(|(i, (name, expr))| {
                let val = self.evaluate(expr, record)?;
                let field_name = name.clone().unwrap_or_else(|| format!("_field{}", i));
                Ok((field_name, val))
            })
            .collect();
        Ok(Value::Struct(struct_fields?))
    }

    fn eval_struct_access(&self, expr: &Expr, field: &str, record: &Record) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        match val {
            Value::Null => Ok(Value::Null),
            Value::Struct(fields) => {
                let field_lower = field.to_lowercase();
                for (name, value) in &fields {
                    if name.to_lowercase() == field_lower {
                        return Ok(value.clone());
                    }
                }
                Ok(Value::Null)
            }
            _ => Err(Error::InvalidQuery(
                "Struct access requires struct value".into(),
            )),
        }
    }

    fn eval_typed_string(&self, data_type: &DataType, value: &str) -> Result<Value> {
        match data_type {
            DataType::Date => parse_date_string(value),
            DataType::Time => parse_time_string(value),
            DataType::Timestamp => parse_timestamp_string(value),
            DataType::DateTime => parse_datetime_string(value),
            DataType::String => Ok(Value::String(value.to_string())),
            DataType::Bytes => Ok(Value::Bytes(value.as_bytes().to_vec())),
            DataType::Json => {
                let json_val: serde_json::Value = serde_json::from_str(value)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?;
                Ok(Value::Json(json_val))
            }
            DataType::Numeric(_) | DataType::BigNumeric => {
                let decimal: rust_decimal::Decimal = value
                    .parse()
                    .map_err(|e| Error::InvalidQuery(format!("Invalid NUMERIC: {}", e)))?;
                Ok(Value::Numeric(decimal))
            }
            DataType::Int64 => {
                let i: i64 = value
                    .parse()
                    .map_err(|e| Error::InvalidQuery(format!("Invalid INT64: {}", e)))?;
                Ok(Value::Int64(i))
            }
            DataType::Float64 => {
                let f: f64 = value
                    .parse()
                    .map_err(|e| Error::InvalidQuery(format!("Invalid FLOAT64: {}", e)))?;
                Ok(Value::Float64(OrderedFloat(f)))
            }
            DataType::Bool => {
                let b: bool = value
                    .parse()
                    .map_err(|e| Error::InvalidQuery(format!("Invalid BOOL: {}", e)))?;
                Ok(Value::Bool(b))
            }
            DataType::Range(inner_type) => parse_range_string(value, inner_type),
            DataType::Unknown
            | DataType::Geography
            | DataType::Struct(_)
            | DataType::Array(_)
            | DataType::Interval => {
                panic!(
                    "[ir_evaluator::eval_typed_string] Unsupported TypedString for data type: {:?}",
                    data_type
                )
            }
        }
    }

    fn eval_interval(
        &self,
        value: &Expr,
        leading_field: Option<DateTimeField>,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(value, record)?;
        match val {
            Value::Int64(n) => {
                let field = leading_field.unwrap_or(DateTimeField::Second);
                interval_from_field(n, field)
            }
            Value::String(s) => parse_interval_string(&s),
            _ => Err(Error::InvalidQuery(
                "INTERVAL requires integer or string value".into(),
            )),
        }
    }

    fn add_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a + b)),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 + b.0))),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 + b.0)))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(a.0 + *b as f64)))
            }
            (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Numeric(*a + *b)),
            (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
            (Value::Date(d), Value::Interval(interval)) => {
                let new_date = add_interval_to_date(d, interval)?;
                Ok(Value::Date(new_date))
            }
            (Value::Interval(interval), Value::Date(d)) => {
                let new_date = add_interval_to_date(d, interval)?;
                Ok(Value::Date(new_date))
            }
            (Value::DateTime(dt), Value::Interval(interval)) => {
                let new_dt = add_interval_to_datetime(dt, interval)?;
                Ok(Value::DateTime(new_dt))
            }
            (Value::Interval(interval), Value::DateTime(dt)) => {
                let new_dt = add_interval_to_datetime(dt, interval)?;
                Ok(Value::DateTime(new_dt))
            }
            (Value::Timestamp(ts), Value::Interval(interval)) => {
                let new_dt = add_interval_to_datetime(&ts.naive_utc(), interval)?;
                Ok(Value::Timestamp(new_dt.and_utc()))
            }
            (Value::Interval(interval), Value::Timestamp(ts)) => {
                let new_dt = add_interval_to_datetime(&ts.naive_utc(), interval)?;
                Ok(Value::Timestamp(new_dt.and_utc()))
            }
            (Value::Interval(a), Value::Interval(b)) => Ok(Value::Interval(IntervalValue {
                months: a.months + b.months,
                days: a.days + b.days,
                nanos: a.nanos + b.nanos,
            })),
            _ => Err(Error::InvalidQuery(format!(
                "Cannot add {:?} and {:?}",
                left, right
            ))),
        }
    }

    fn sub_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a - b)),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 - b.0))),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 - b.0)))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(a.0 - *b as f64)))
            }
            (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Numeric(*a - *b)),
            (Value::Date(d), Value::Interval(interval)) => {
                let neg_interval = negate_interval(interval);
                let new_date = add_interval_to_date(d, &neg_interval)?;
                Ok(Value::Date(new_date))
            }
            (Value::DateTime(dt), Value::Interval(interval)) => {
                let neg_interval = negate_interval(interval);
                let new_dt = add_interval_to_datetime(dt, &neg_interval)?;
                Ok(Value::DateTime(new_dt))
            }
            (Value::Timestamp(ts), Value::Interval(interval)) => {
                let neg_interval = negate_interval(interval);
                let new_dt = add_interval_to_datetime(&ts.naive_utc(), &neg_interval)?;
                Ok(Value::Timestamp(new_dt.and_utc()))
            }
            (Value::Interval(a), Value::Interval(b)) => Ok(Value::Interval(IntervalValue {
                months: a.months - b.months,
                days: a.days - b.days,
                nanos: a.nanos - b.nanos,
            })),
            (Value::Float64(a), Value::String(s)) => {
                if let Ok(b) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a.0 - b)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot subtract {:?} from {:?}",
                        right, left
                    )))
                }
            }
            (Value::String(s), Value::Float64(b)) => {
                if let Ok(a) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a - b.0)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot subtract {:?} from {:?}",
                        right, left
                    )))
                }
            }
            (Value::Int64(a), Value::String(s)) => {
                if let Ok(b) = s.parse::<i64>() {
                    Ok(Value::Int64(a - b))
                } else if let Ok(b) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(*a as f64 - b)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot subtract {:?} from {:?}",
                        right, left
                    )))
                }
            }
            (Value::String(s), Value::Int64(b)) => {
                if let Ok(a) = s.parse::<i64>() {
                    Ok(Value::Int64(a - b))
                } else if let Ok(a) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a - *b as f64)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot subtract {:?} from {:?}",
                        right, left
                    )))
                }
            }
            (Value::String(s1), Value::String(s2)) => {
                if let (Ok(a), Ok(b)) = (s1.parse::<f64>(), s2.parse::<f64>()) {
                    Ok(Value::Float64(OrderedFloat(a - b)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot subtract {:?} from {:?}",
                        right, left
                    )))
                }
            }
            _ => Err(Error::InvalidQuery(format!(
                "Cannot subtract {:?} from {:?}",
                right, left
            ))),
        }
    }

    fn mul_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a * b)),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 * b.0))),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 * b.0)))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(a.0 * *b as f64)))
            }
            (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Numeric(*a * *b)),
            (Value::Interval(iv), Value::Int64(n)) => Ok(Value::Interval(IntervalValue {
                months: iv.months * (*n as i32),
                days: iv.days * (*n as i32),
                nanos: iv.nanos * *n,
            })),
            (Value::Int64(n), Value::Interval(iv)) => Ok(Value::Interval(IntervalValue {
                months: iv.months * (*n as i32),
                days: iv.days * (*n as i32),
                nanos: iv.nanos * *n,
            })),
            (Value::String(s1), Value::String(s2)) => {
                if let (Ok(a), Ok(b)) = (s1.parse::<f64>(), s2.parse::<f64>()) {
                    Ok(Value::Float64(OrderedFloat(a * b)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot multiply {:?} and {:?}",
                        left, right
                    )))
                }
            }
            (Value::String(s), Value::Int64(b)) => {
                if let Ok(a) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a * *b as f64)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot multiply {:?} and {:?}",
                        left, right
                    )))
                }
            }
            (Value::Int64(a), Value::String(s)) => {
                if let Ok(b) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(*a as f64 * b)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot multiply {:?} and {:?}",
                        left, right
                    )))
                }
            }
            (Value::String(s), Value::Float64(b)) => {
                if let Ok(a) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a * b.0)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot multiply {:?} and {:?}",
                        left, right
                    )))
                }
            }
            (Value::Float64(a), Value::String(s)) => {
                if let Ok(b) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a.0 * b)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot multiply {:?} and {:?}",
                        left, right
                    )))
                }
            }
            _ => Err(Error::InvalidQuery(format!(
                "Cannot multiply {:?} and {:?}",
                left, right
            ))),
        }
    }

    fn div_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (_, Value::Int64(0)) => Err(Error::InvalidQuery("Division by zero".into())),
            (_, Value::Float64(f)) if f.0 == 0.0 => {
                Err(Error::InvalidQuery("Division by zero".into()))
            }
            (Value::Int64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 / *b as f64)))
            }
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 / b.0))),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 / b.0)))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(a.0 / *b as f64)))
            }
            (Value::Numeric(a), Value::Numeric(b)) => {
                if b.is_zero() {
                    Err(Error::InvalidQuery("Division by zero".into()))
                } else {
                    Ok(Value::Numeric(*a / *b))
                }
            }
            (Value::Int64(a), Value::String(s)) => {
                if let Ok(b) = s.parse::<f64>() {
                    if b == 0.0 {
                        Err(Error::InvalidQuery("Division by zero".into()))
                    } else {
                        Ok(Value::Float64(OrderedFloat(*a as f64 / b)))
                    }
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot divide {:?} by {:?}",
                        left, right
                    )))
                }
            }
            (Value::String(s), Value::Int64(b)) => {
                if *b == 0 {
                    return Err(Error::InvalidQuery("Division by zero".into()));
                }
                if let Ok(a) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a / *b as f64)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot divide {:?} by {:?}",
                        left, right
                    )))
                }
            }
            (Value::Float64(a), Value::String(s)) => {
                if let Ok(b) = s.parse::<f64>() {
                    if b == 0.0 {
                        Err(Error::InvalidQuery("Division by zero".into()))
                    } else {
                        Ok(Value::Float64(OrderedFloat(a.0 / b)))
                    }
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot divide {:?} by {:?}",
                        left, right
                    )))
                }
            }
            (Value::String(s), Value::Float64(b)) => {
                if b.0 == 0.0 {
                    return Err(Error::InvalidQuery("Division by zero".into()));
                }
                if let Ok(a) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a / b.0)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot divide {:?} by {:?}",
                        left, right
                    )))
                }
            }
            (Value::String(s1), Value::String(s2)) => {
                if let (Ok(a), Ok(b)) = (s1.parse::<f64>(), s2.parse::<f64>()) {
                    if b == 0.0 {
                        Err(Error::InvalidQuery("Division by zero".into()))
                    } else {
                        Ok(Value::Float64(OrderedFloat(a / b)))
                    }
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot divide {:?} by {:?}",
                        left, right
                    )))
                }
            }
            _ => Err(Error::InvalidQuery(format!(
                "Cannot divide {:?} by {:?}",
                left, right
            ))),
        }
    }

    fn mod_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (_, Value::Int64(0)) => Err(Error::InvalidQuery("Modulo by zero".into())),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a % b)),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 % b.0))),
            (Value::Numeric(a), Value::Numeric(b)) => {
                if b.is_zero() {
                    Err(Error::InvalidQuery("Modulo by zero".into()))
                } else {
                    Ok(Value::Numeric(*a % *b))
                }
            }
            _ => Err(Error::InvalidQuery(format!(
                "Cannot compute modulo of {:?} and {:?}",
                left, right
            ))),
        }
    }

    fn values_equal(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => false,
            (Value::Int64(a), Value::Float64(b)) => (*a as f64 - b.0).abs() < f64::EPSILON,
            (Value::Float64(a), Value::Int64(b)) => (a.0 - *b as f64).abs() < f64::EPSILON,
            _ => left == right,
        }
    }

    fn eq_values(&self, left: &Value, right: &Value) -> Result<Value> {
        self.eq_values_with_collation(left, right, None)
    }

    fn eq_values_with_collation(
        &self,
        left: &Value,
        right: &Value,
        collation: Option<&str>,
    ) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Bool((*a as f64 - b.0).abs() < f64::EPSILON))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Bool((a.0 - *b as f64).abs() < f64::EPSILON))
            }
            (Value::String(a), Value::String(b)) if matches!(collation, Some("und:ci")) => {
                Ok(Value::Bool(a.to_lowercase() == b.to_lowercase()))
            }
            _ => Ok(Value::Bool(left == right)),
        }
    }

    fn neq_values(&self, left: &Value, right: &Value) -> Result<Value> {
        self.neq_values_with_collation(left, right, None)
    }

    fn neq_values_with_collation(
        &self,
        left: &Value,
        right: &Value,
        collation: Option<&str>,
    ) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Bool((*a as f64 - b.0).abs() >= f64::EPSILON))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Bool((a.0 - *b as f64).abs() >= f64::EPSILON))
            }
            (Value::String(a), Value::String(b)) if matches!(collation, Some("und:ci")) => {
                Ok(Value::Bool(a.to_lowercase() != b.to_lowercase()))
            }
            _ => Ok(Value::Bool(left != right)),
        }
    }

    fn compare_values<F>(&self, left: &Value, right: &Value, cmp: F) -> Result<Value>
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Bool(cmp(a
                .partial_cmp(b)
                .unwrap_or(std::cmp::Ordering::Equal)))),
            (Value::Int64(a), Value::Float64(b)) => {
                let af = *a as f64;
                Ok(Value::Bool(cmp(af
                    .partial_cmp(&b.0)
                    .unwrap_or(std::cmp::Ordering::Equal))))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                let bf = *b as f64;
                Ok(Value::Bool(cmp(a
                    .0
                    .partial_cmp(&bf)
                    .unwrap_or(std::cmp::Ordering::Equal))))
            }
            (Value::String(a), Value::String(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Bytes(a), Value::Bytes(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Date(a), Value::Date(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Time(a), Value::Time(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::DateTime(a), Value::DateTime(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Timestamp(a), Value::Timestamp(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Numeric(a), Value::Int64(b)) => {
                let b_decimal = rust_decimal::Decimal::from(*b);
                Ok(Value::Bool(cmp(a.cmp(&b_decimal))))
            }
            (Value::Int64(a), Value::Numeric(b)) => {
                let a_decimal = rust_decimal::Decimal::from(*a);
                Ok(Value::Bool(cmp(a_decimal.cmp(b))))
            }
            (Value::Numeric(a), Value::Float64(b)) => {
                if let Some(a_f64) = a.to_f64() {
                    Ok(Value::Bool(cmp(a_f64
                        .partial_cmp(&b.0)
                        .unwrap_or(std::cmp::Ordering::Equal))))
                } else {
                    Ok(Value::Null)
                }
            }
            (Value::Float64(a), Value::Numeric(b)) => {
                if let Some(b_f64) = b.to_f64() {
                    Ok(Value::Bool(cmp(a
                        .0
                        .partial_cmp(&b_f64)
                        .unwrap_or(std::cmp::Ordering::Equal))))
                } else {
                    Ok(Value::Null)
                }
            }
            (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Interval(a), Value::Interval(b)) => {
                let a_total_nanos = a.nanos
                    + a.days as i64 * 86_400_000_000_000
                    + a.months as i64 * 30 * 86_400_000_000_000;
                let b_total_nanos = b.nanos
                    + b.days as i64 * 86_400_000_000_000
                    + b.months as i64 * 30 * 86_400_000_000_000;
                Ok(Value::Bool(cmp(a_total_nanos.cmp(&b_total_nanos))))
            }
            _ => Err(Error::InvalidQuery(format!(
                "Cannot compare {:?} with {:?}",
                left, right
            ))),
        }
    }

    fn and_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
            (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
            (Value::Bool(true), Value::Null) | (Value::Null, Value::Bool(true)) => Ok(Value::Null),
            (Value::Null, Value::Null) => Ok(Value::Null),
            _ => Err(Error::InvalidQuery("AND requires boolean operands".into())),
        }
    }

    fn or_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
            (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
            (Value::Bool(false), Value::Null) | (Value::Null, Value::Bool(false)) => {
                Ok(Value::Null)
            }
            (Value::Null, Value::Null) => Ok(Value::Null),
            _ => Err(Error::InvalidQuery("OR requires boolean operands".into())),
        }
    }

    fn concat_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
            (Value::Array(a), Value::Array(b)) => {
                let mut result = a.clone();
                result.extend(b.clone());
                Ok(Value::Array(result))
            }
            _ => Err(Error::InvalidQuery(
                "Concat requires string or array operands".into(),
            )),
        }
    }

    fn bitwise_and(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a & b)),
            _ => Err(Error::InvalidQuery(
                "Bitwise AND requires integer operands".into(),
            )),
        }
    }

    fn bitwise_or(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a | b)),
            _ => Err(Error::InvalidQuery(
                "Bitwise OR requires integer operands".into(),
            )),
        }
    }

    fn bitwise_xor(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a ^ b)),
            _ => Err(Error::InvalidQuery(
                "Bitwise XOR requires integer operands".into(),
            )),
        }
    }

    fn shift_left(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a << b)),
            _ => Err(Error::InvalidQuery(
                "Shift left requires integer operands".into(),
            )),
        }
    }

    fn shift_right(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a >> b)),
            _ => Err(Error::InvalidQuery(
                "Shift right requires integer operands".into(),
            )),
        }
    }

    fn fn_coalesce(&self, args: &[Value]) -> Result<Value> {
        for arg in args {
            if !arg.is_null() {
                return Ok(arg.clone());
            }
        }
        Ok(Value::Null)
    }

    fn fn_ifnull(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("IFNULL requires 2 arguments".into()));
        }
        if args[0].is_null() {
            Ok(args[1].clone())
        } else {
            Ok(args[0].clone())
        }
    }

    fn fn_nullif(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("NULLIF requires 2 arguments".into()));
        }
        if self.values_equal(&args[0], &args[1]) {
            Ok(Value::Null)
        } else {
            Ok(args[0].clone())
        }
    }

    fn fn_if(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery("IF requires 3 arguments".into()));
        }
        match &args[0] {
            Value::Bool(true) => Ok(args[1].clone()),
            Value::Bool(false) | Value::Null => Ok(args[2].clone()),
            _ => Err(Error::InvalidQuery("IF requires boolean condition".into())),
        }
    }

    fn fn_generate_date_array(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "GENERATE_DATE_ARRAY requires at least 2 arguments".into(),
            ));
        }
        let start_date = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Date(d) => *d,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_DATE_ARRAY requires DATE arguments".into(),
                ));
            }
        };
        let end_date = match &args[1] {
            Value::Null => return Ok(Value::Null),
            Value::Date(d) => *d,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_DATE_ARRAY requires DATE arguments".into(),
                ));
            }
        };
        let step_days = match args.get(2) {
            Some(Value::Null) => return Ok(Value::Null),
            Some(Value::Interval(iv)) => iv.days as i64 + iv.months as i64 * 30,
            Some(Value::Int64(n)) => *n,
            None => 1,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_DATE_ARRAY step must be INTERVAL or INT64".into(),
                ));
            }
        };
        if step_days == 0 {
            return Err(Error::InvalidQuery(
                "GENERATE_DATE_ARRAY step cannot be zero".into(),
            ));
        }
        let mut result = Vec::new();
        let mut current = start_date;
        if step_days > 0 {
            while current <= end_date {
                result.push(Value::Date(current));
                current = current + chrono::Days::new(step_days as u64);
            }
        } else {
            let neg_step = (-step_days) as u64;
            while current >= end_date {
                result.push(Value::Date(current));
                current = current - chrono::Days::new(neg_step);
            }
        }
        Ok(Value::Array(result))
    }

    fn fn_generate_timestamp_array(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "GENERATE_TIMESTAMP_ARRAY requires 3 arguments".into(),
            ));
        }
        let start_ts = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Timestamp(ts) => *ts,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_TIMESTAMP_ARRAY requires TIMESTAMP arguments".into(),
                ));
            }
        };
        let end_ts = match &args[1] {
            Value::Null => return Ok(Value::Null),
            Value::Timestamp(ts) => *ts,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_TIMESTAMP_ARRAY requires TIMESTAMP arguments".into(),
                ));
            }
        };
        let step = match &args[2] {
            Value::Null => return Ok(Value::Null),
            Value::Interval(iv) => iv,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_TIMESTAMP_ARRAY step must be INTERVAL".into(),
                ));
            }
        };
        let step_nanos = step.nanos
            + step.days as i64 * 86_400_000_000_000
            + step.months as i64 * 30 * 86_400_000_000_000;
        if step_nanos == 0 {
            return Err(Error::InvalidQuery(
                "GENERATE_TIMESTAMP_ARRAY step cannot be zero".into(),
            ));
        }
        let mut result = Vec::new();
        let mut current = start_ts;
        if step_nanos > 0 {
            while current <= end_ts {
                result.push(Value::Timestamp(current));
                current += chrono::Duration::nanoseconds(step_nanos);
            }
        } else {
            while current >= end_ts {
                result.push(Value::Timestamp(current));
                current += chrono::Duration::nanoseconds(step_nanos);
            }
        }
        Ok(Value::Array(result))
    }

    fn fn_farm_fingerprint(&self, args: &[Value]) -> Result<Value> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let mut hasher = DefaultHasher::new();
                s.hash(&mut hasher);
                let hash = hasher.finish() as i64;
                Ok(Value::Int64(hash))
            }
            Some(Value::Bytes(b)) => {
                let mut hasher = DefaultHasher::new();
                b.hash(&mut hasher);
                let hash = hasher.finish() as i64;
                Ok(Value::Int64(hash))
            }
            _ => Err(Error::InvalidQuery(
                "FARM_FINGERPRINT requires STRING or BYTES argument".into(),
            )),
        }
    }

    fn fn_generate_uuid(&self, _args: &[Value]) -> Result<Value> {
        Ok(Value::String(uuid::Uuid::new_v4().to_string()))
    }

    fn fn_zeroifnull(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) | None => Ok(Value::Int64(0)),
            Some(v) => Ok(v.clone()),
        }
    }

    fn fn_string(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Json(j)) => {
                if let Some(s) = j.as_str() {
                    Ok(Value::String(s.to_string()))
                } else {
                    Ok(Value::String(j.to_string()))
                }
            }
            Some(v) => Ok(Value::String(format!("{}", v))),
            None => Err(Error::InvalidQuery("STRING requires an argument".into())),
        }
    }

    fn fn_sin(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).sin()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.sin()))),
            _ => Err(Error::InvalidQuery("SIN requires numeric argument".into())),
        }
    }

    fn fn_cos(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).cos()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.cos()))),
            _ => Err(Error::InvalidQuery("COS requires numeric argument".into())),
        }
    }

    fn fn_tan(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).tan()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.tan()))),
            _ => Err(Error::InvalidQuery("TAN requires numeric argument".into())),
        }
    }

    fn fn_asin(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).asin()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.asin()))),
            _ => Err(Error::InvalidQuery("ASIN requires numeric argument".into())),
        }
    }

    fn fn_acos(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).acos()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.acos()))),
            _ => Err(Error::InvalidQuery("ACOS requires numeric argument".into())),
        }
    }

    fn fn_atan(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).atan()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.atan()))),
            _ => Err(Error::InvalidQuery("ATAN requires numeric argument".into())),
        }
    }

    fn fn_atan2(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("ATAN2 requires 2 arguments".into()));
        }
        let y = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Int64(n) => *n as f64,
            Value::Float64(f) => f.0,
            _ => {
                return Err(Error::InvalidQuery(
                    "ATAN2 requires numeric arguments".into(),
                ));
            }
        };
        let x = match &args[1] {
            Value::Null => return Ok(Value::Null),
            Value::Int64(n) => *n as f64,
            Value::Float64(f) => f.0,
            _ => {
                return Err(Error::InvalidQuery(
                    "ATAN2 requires numeric arguments".into(),
                ));
            }
        };
        Ok(Value::Float64(OrderedFloat(y.atan2(x))))
    }

    fn fn_sinh(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).sinh()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.sinh()))),
            _ => Err(Error::InvalidQuery("SINH requires numeric argument".into())),
        }
    }

    fn fn_cosh(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).cosh()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.cosh()))),
            _ => Err(Error::InvalidQuery("COSH requires numeric argument".into())),
        }
    }

    fn fn_tanh(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).tanh()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.tanh()))),
            _ => Err(Error::InvalidQuery("TANH requires numeric argument".into())),
        }
    }

    fn fn_asinh(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).asinh()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.asinh()))),
            _ => Err(Error::InvalidQuery(
                "ASINH requires numeric argument".into(),
            )),
        }
    }

    fn fn_acosh(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let val = *n as f64;
                if val < 1.0 {
                    Err(Error::InvalidQuery("ACOSH argument must be >= 1".into()))
                } else {
                    Ok(Value::Float64(OrderedFloat(val.acosh())))
                }
            }
            Some(Value::Float64(f)) => {
                if f.0 < 1.0 {
                    Err(Error::InvalidQuery("ACOSH argument must be >= 1".into()))
                } else {
                    Ok(Value::Float64(OrderedFloat(f.0.acosh())))
                }
            }
            _ => Err(Error::InvalidQuery(
                "ACOSH requires numeric argument".into(),
            )),
        }
    }

    fn fn_atanh(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let val = *n as f64;
                if val <= -1.0 || val >= 1.0 {
                    Err(Error::InvalidQuery(
                        "ATANH argument must be in (-1, 1)".into(),
                    ))
                } else {
                    Ok(Value::Float64(OrderedFloat(val.atanh())))
                }
            }
            Some(Value::Float64(f)) => {
                if f.0 <= -1.0 || f.0 >= 1.0 {
                    Err(Error::InvalidQuery(
                        "ATANH argument must be in (-1, 1)".into(),
                    ))
                } else {
                    Ok(Value::Float64(OrderedFloat(f.0.atanh())))
                }
            }
            _ => Err(Error::InvalidQuery(
                "ATANH requires numeric argument".into(),
            )),
        }
    }

    fn fn_cot(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let x = *n as f64;
                Ok(Value::Float64(OrderedFloat(x.cos() / x.sin())))
            }
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.cos() / f.0.sin()))),
            _ => Err(Error::InvalidQuery("COT requires numeric argument".into())),
        }
    }

    fn fn_csc(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let x = *n as f64;
                Ok(Value::Float64(OrderedFloat(1.0 / x.sin())))
            }
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.sin()))),
            _ => Err(Error::InvalidQuery("CSC requires numeric argument".into())),
        }
    }

    fn fn_sec(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let x = *n as f64;
                Ok(Value::Float64(OrderedFloat(1.0 / x.cos())))
            }
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.cos()))),
            _ => Err(Error::InvalidQuery("SEC requires numeric argument".into())),
        }
    }

    fn fn_coth(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let x = *n as f64;
                Ok(Value::Float64(OrderedFloat(1.0 / x.tanh())))
            }
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.tanh()))),
            _ => Err(Error::InvalidQuery("COTH requires numeric argument".into())),
        }
    }

    fn fn_csch(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let x = *n as f64;
                Ok(Value::Float64(OrderedFloat(1.0 / x.sinh())))
            }
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.sinh()))),
            _ => Err(Error::InvalidQuery("CSCH requires numeric argument".into())),
        }
    }

    fn fn_sech(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let x = *n as f64;
                Ok(Value::Float64(OrderedFloat(1.0 / x.cosh())))
            }
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.cosh()))),
            _ => Err(Error::InvalidQuery("SECH requires numeric argument".into())),
        }
    }

    fn fn_is_nan(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Float64(f)) => Ok(Value::Bool(f.0.is_nan())),
            Some(Value::Int64(_)) => Ok(Value::Bool(false)),
            _ => Err(Error::InvalidQuery(
                "IS_NAN requires numeric argument".into(),
            )),
        }
    }

    fn fn_is_inf(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Float64(f)) => Ok(Value::Bool(f.0.is_infinite())),
            Some(Value::Int64(_)) => Ok(Value::Bool(false)),
            _ => Err(Error::InvalidQuery(
                "IS_INF requires numeric argument".into(),
            )),
        }
    }

    fn fn_cosine_distance(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::InvalidQuery(
                "COSINE_DISTANCE requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Array(a), Value::Array(b)) => {
                if a.len() != b.len() {
                    return Err(Error::InvalidQuery(
                        "COSINE_DISTANCE: arrays must have same length".into(),
                    ));
                }
                let mut dot_product = 0.0;
                let mut norm_a = 0.0;
                let mut norm_b = 0.0;
                for (va, vb) in a.iter().zip(b.iter()) {
                    let fa = match va {
                        Value::Float64(f) => f.0,
                        Value::Int64(n) => *n as f64,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(Error::InvalidQuery(
                                "COSINE_DISTANCE: array elements must be numeric".into(),
                            ));
                        }
                    };
                    let fb = match vb {
                        Value::Float64(f) => f.0,
                        Value::Int64(n) => *n as f64,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(Error::InvalidQuery(
                                "COSINE_DISTANCE: array elements must be numeric".into(),
                            ));
                        }
                    };
                    dot_product += fa * fb;
                    norm_a += fa * fa;
                    norm_b += fb * fb;
                }
                let similarity = dot_product / (norm_a.sqrt() * norm_b.sqrt());
                Ok(Value::Float64(OrderedFloat(1.0 - similarity)))
            }
            _ => Err(Error::InvalidQuery(
                "COSINE_DISTANCE requires array arguments".into(),
            )),
        }
    }

    fn fn_euclidean_distance(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::InvalidQuery(
                "EUCLIDEAN_DISTANCE requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Array(a), Value::Array(b)) => {
                if a.len() != b.len() {
                    return Err(Error::InvalidQuery(
                        "EUCLIDEAN_DISTANCE: arrays must have same length".into(),
                    ));
                }
                let mut sum_sq = 0.0;
                for (va, vb) in a.iter().zip(b.iter()) {
                    let fa = match va {
                        Value::Float64(f) => f.0,
                        Value::Int64(n) => *n as f64,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(Error::InvalidQuery(
                                "EUCLIDEAN_DISTANCE: array elements must be numeric".into(),
                            ));
                        }
                    };
                    let fb = match vb {
                        Value::Float64(f) => f.0,
                        Value::Int64(n) => *n as f64,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(Error::InvalidQuery(
                                "EUCLIDEAN_DISTANCE: array elements must be numeric".into(),
                            ));
                        }
                    };
                    let diff = fa - fb;
                    sum_sq += diff * diff;
                }
                Ok(Value::Float64(OrderedFloat(sum_sq.sqrt())))
            }
            _ => Err(Error::InvalidQuery(
                "EUCLIDEAN_DISTANCE requires array arguments".into(),
            )),
        }
    }

    fn fn_range_bucket(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::InvalidQuery(
                "RANGE_BUCKET requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) => Ok(Value::Null),
            (_, Value::Null) => Ok(Value::Null),
            (point, Value::Array(arr)) => {
                if arr.is_empty() {
                    return Ok(Value::Int64(0));
                }
                let point_val = match point {
                    Value::Int64(n) => *n as f64,
                    Value::Float64(f) => f.0,
                    _ => {
                        return Err(Error::InvalidQuery(
                            "RANGE_BUCKET: point must be numeric".into(),
                        ));
                    }
                };
                let mut bucket = 0i64;
                for v in arr {
                    let boundary = match v {
                        Value::Int64(n) => *n as f64,
                        Value::Float64(f) => f.0,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(Error::InvalidQuery(
                                "RANGE_BUCKET: array elements must be numeric".into(),
                            ));
                        }
                    };
                    if point_val < boundary {
                        break;
                    }
                    bucket += 1;
                }
                Ok(Value::Int64(bucket))
            }
            _ => Err(Error::InvalidQuery(
                "RANGE_BUCKET requires point and array arguments".into(),
            )),
        }
    }

    fn fn_rand(&self, _args: &[Value]) -> Result<Value> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let random = (seed as f64 / u64::MAX as f64).fract();
        Ok(Value::Float64(OrderedFloat(random)))
    }

    fn format_with_grouping(n: i64) -> String {
        let sign = if n < 0 { "-" } else { "" };
        let abs_n = n.abs();
        let s = abs_n.to_string();
        let mut result = String::new();
        for (i, c) in s.chars().rev().enumerate() {
            if i > 0 && i % 3 == 0 {
                result.push(',');
            }
            result.push(c);
        }
        format!("{}{}", sign, result.chars().rev().collect::<String>())
    }

    fn format_scientific(f: f64, precision: usize, uppercase: bool) -> String {
        if f == 0.0 {
            let e_char = if uppercase { 'E' } else { 'e' };
            return format!("{:.prec$}{}+00", 0.0, e_char, prec = precision);
        }
        let exp = f.abs().log10().floor() as i32;
        let mantissa = f / 10_f64.powi(exp);
        let e_char = if uppercase { 'E' } else { 'e' };
        let sign = if exp >= 0 { '+' } else { '-' };
        format!(
            "{:.prec$}{}{}{:02}",
            mantissa,
            e_char,
            sign,
            exp.abs(),
            prec = precision
        )
    }

    fn format_value_for_format(&self, value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int64(n) => n.to_string(),
            Value::Float64(f) => f.0.to_string(),
            Value::Numeric(n) => n.to_string(),
            Value::BigNumeric(n) => n.to_string(),
            Value::String(s) => s.clone(),
            Value::Bytes(b) => format!("{:?}", b),
            Value::Date(d) => d.to_string(),
            Value::Time(t) => t.to_string(),
            Value::DateTime(dt) => dt.to_string(),
            Value::Timestamp(ts) => ts.to_rfc3339(),
            Value::Json(j) => j.to_string(),
            Value::Array(arr) => format!("{:?}", arr),
            Value::Struct(fields) => format!("{:?}", fields),
            Value::Geography(g) => g.clone(),
            Value::Interval(i) => format!("{:?}", i),
            Value::Range(r) => format!("{:?}", r),
            Value::Default => "DEFAULT".to_string(),
        }
    }

    fn fn_cast(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("CAST requires an argument".into()));
        }
        Ok(args[0].clone())
    }

    fn fn_to_base64(&self, args: &[Value]) -> Result<Value> {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::String(STANDARD.encode(s.as_bytes()))),
            Some(Value::Bytes(b)) => Ok(Value::String(STANDARD.encode(b))),
            _ => Err(Error::InvalidQuery(
                "TO_BASE64 requires string or bytes argument".into(),
            )),
        }
    }

    fn fn_from_base64(&self, args: &[Value]) -> Result<Value> {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let decoded = STANDARD
                    .decode(s)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid base64: {}", e)))?;
                Ok(Value::Bytes(decoded))
            }
            _ => Err(Error::InvalidQuery(
                "FROM_BASE64 requires string argument".into(),
            )),
        }
    }

    fn fn_to_base32(&self, args: &[Value]) -> Result<Value> {
        use data_encoding::BASE32;
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Bytes(b)) => Ok(Value::String(BASE32.encode(b))),
            Some(Value::String(s)) => Ok(Value::String(BASE32.encode(s.as_bytes()))),
            _ => Err(Error::InvalidQuery(
                "TO_BASE32 requires bytes or string argument".into(),
            )),
        }
    }

    fn fn_from_base32(&self, args: &[Value]) -> Result<Value> {
        use data_encoding::BASE32;
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let decoded = BASE32
                    .decode(s.as_bytes())
                    .map_err(|e| Error::InvalidQuery(format!("Invalid base32: {}", e)))?;
                Ok(Value::Bytes(decoded))
            }
            _ => Err(Error::InvalidQuery(
                "FROM_BASE32 requires string argument".into(),
            )),
        }
    }

    fn fn_to_hex(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Bytes(b)) => Ok(Value::String(hex::encode(b))),
            Some(Value::String(s)) => Ok(Value::String(hex::encode(s.as_bytes()))),
            _ => Err(Error::InvalidQuery("TO_HEX requires bytes argument".into())),
        }
    }

    fn fn_from_hex(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let normalized = if s.len() % 2 == 1 {
                    format!("0{}", s)
                } else {
                    s.clone()
                };
                let decoded = hex::decode(&normalized)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid hex: {}", e)))?;
                Ok(Value::Bytes(decoded))
            }
            _ => Err(Error::InvalidQuery(
                "FROM_HEX requires string argument".into(),
            )),
        }
    }

    fn fn_safe_convert_bytes_to_string(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "SAFE_CONVERT_BYTES_TO_STRING requires 1 argument".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(b) => match String::from_utf8(b.clone()) {
                Ok(s) => Ok(Value::String(s)),
                Err(_) => Ok(Value::Null),
            },
            Value::Bool(_)
            | Value::Int64(_)
            | Value::Float64(_)
            | Value::Numeric(_)
            | Value::BigNumeric(_)
            | Value::String(_)
            | Value::Date(_)
            | Value::Time(_)
            | Value::DateTime(_)
            | Value::Timestamp(_)
            | Value::Json(_)
            | Value::Array(_)
            | Value::Struct(_)
            | Value::Geography(_)
            | Value::Interval(_)
            | Value::Range(_)
            | Value::Default => Err(Error::InvalidQuery(
                "SAFE_CONVERT_BYTES_TO_STRING requires BYTES argument".into(),
            )),
        }
    }

    fn fn_convert_bytes_to_string(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "CONVERT_BYTES_TO_STRING requires 1 argument".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(b) => {
                let s = String::from_utf8(b.clone())
                    .map_err(|e| Error::InvalidQuery(format!("Invalid UTF-8 in bytes: {}", e)))?;
                Ok(Value::String(s))
            }
            Value::Bool(_)
            | Value::Int64(_)
            | Value::Float64(_)
            | Value::Numeric(_)
            | Value::BigNumeric(_)
            | Value::String(_)
            | Value::Date(_)
            | Value::Time(_)
            | Value::DateTime(_)
            | Value::Timestamp(_)
            | Value::Json(_)
            | Value::Array(_)
            | Value::Struct(_)
            | Value::Geography(_)
            | Value::Interval(_)
            | Value::Range(_)
            | Value::Default => Err(Error::InvalidQuery(
                "CONVERT_BYTES_TO_STRING requires BYTES argument".into(),
            )),
        }
    }

    fn fn_typeof(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::String("NULL".to_string())),
            Some(Value::Bool(_)) => Ok(Value::String("BOOL".to_string())),
            Some(Value::Int64(_)) => Ok(Value::String("INT64".to_string())),
            Some(Value::Float64(_)) => Ok(Value::String("FLOAT64".to_string())),
            Some(Value::String(_)) => Ok(Value::String("STRING".to_string())),
            Some(Value::Bytes(_)) => Ok(Value::String("BYTES".to_string())),
            Some(Value::Date(_)) => Ok(Value::String("DATE".to_string())),
            Some(Value::Time(_)) => Ok(Value::String("TIME".to_string())),
            Some(Value::DateTime(_)) => Ok(Value::String("DATETIME".to_string())),
            Some(Value::Timestamp(_)) => Ok(Value::String("TIMESTAMP".to_string())),
            Some(Value::Array(_)) => Ok(Value::String("ARRAY".to_string())),
            Some(Value::Struct(_)) => Ok(Value::String("STRUCT".to_string())),
            Some(Value::Json(_)) => Ok(Value::String("JSON".to_string())),
            Some(Value::Numeric(_)) => Ok(Value::String("NUMERIC".to_string())),
            Some(Value::BigNumeric(_)) => Ok(Value::String("BIGNUMERIC".to_string())),
            Some(Value::Interval(_)) => Ok(Value::String("INTERVAL".to_string())),
            Some(Value::Geography(_)) => Ok(Value::String("GEOGRAPHY".to_string())),
            Some(Value::Range(_)) => Ok(Value::String("RANGE".to_string())),
            Some(Value::Default) => Ok(Value::String("DEFAULT".to_string())),
            None => Ok(Value::Null),
        }
    }

    fn fn_nvl2(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery("NVL2 requires 3 arguments".into()));
        }
        if args[0].is_null() {
            Ok(args[2].clone())
        } else {
            Ok(args[1].clone())
        }
    }

    fn fn_range(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::InvalidQuery("RANGE requires 2 arguments".into()));
        }
        let start = if args[0].is_null() {
            None
        } else {
            Some(args[0].clone())
        };
        let end = if args[1].is_null() {
            None
        } else {
            Some(args[1].clone())
        };
        Ok(Value::range(start, end))
    }

    fn fn_bit_count(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("BIT_COUNT requires 1 argument".into()));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Int64(n) => {
                let count = n.count_ones() as i64;
                Ok(Value::Int64(count))
            }
            Value::Bytes(bytes) => {
                let count: u32 = bytes.iter().map(|b| b.count_ones()).sum();
                Ok(Value::Int64(count as i64))
            }
            _ => Err(Error::InvalidQuery(
                "BIT_COUNT requires an integer or bytes argument".into(),
            )),
        }
    }

    fn fn_int64_from_json(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("INT64 requires 1 argument".into()));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(json) => match json {
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Ok(Value::Int64(i))
                    } else if let Some(f) = n.as_f64() {
                        Ok(Value::Int64(f as i64))
                    } else {
                        Err(Error::InvalidQuery(
                            "Cannot convert JSON number to INT64".into(),
                        ))
                    }
                }
                serde_json::Value::String(s) => s
                    .parse::<i64>()
                    .map(Value::Int64)
                    .map_err(|_| Error::InvalidQuery("Cannot parse JSON string as INT64".into())),
                serde_json::Value::Bool(b) => Ok(Value::Int64(if *b { 1 } else { 0 })),
                _ => Err(Error::InvalidQuery("Cannot convert JSON to INT64".into())),
            },
            Value::Int64(n) => Ok(Value::Int64(*n)),
            Value::Float64(f) => Ok(Value::Int64(f.0 as i64)),
            Value::String(s) => s
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|_| Error::InvalidQuery("Cannot parse string as INT64".into())),
            _ => Err(Error::InvalidQuery("INT64 requires a JSON argument".into())),
        }
    }

    fn fn_float64_from_json(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("FLOAT64 requires 1 argument".into()));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(json) => match json {
                serde_json::Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        Ok(Value::Float64(OrderedFloat(f)))
                    } else {
                        Err(Error::InvalidQuery(
                            "Cannot convert JSON number to FLOAT64".into(),
                        ))
                    }
                }
                serde_json::Value::String(s) => s
                    .parse::<f64>()
                    .map(|f| Value::Float64(OrderedFloat(f)))
                    .map_err(|_| Error::InvalidQuery("Cannot parse JSON string as FLOAT64".into())),
                _ => Err(Error::InvalidQuery("Cannot convert JSON to FLOAT64".into())),
            },
            Value::Float64(f) => Ok(Value::Float64(*f)),
            Value::Int64(n) => Ok(Value::Float64(OrderedFloat(*n as f64))),
            Value::String(s) => s
                .parse::<f64>()
                .map(|f| Value::Float64(OrderedFloat(f)))
                .map_err(|_| Error::InvalidQuery("Cannot parse string as FLOAT64".into())),
            _ => Err(Error::InvalidQuery(
                "FLOAT64 requires a JSON argument".into(),
            )),
        }
    }

    fn fn_bool_from_json(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("BOOL requires 1 argument".into()));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(json) => match json {
                serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
                serde_json::Value::String(s) => match s.to_lowercase().as_str() {
                    "true" => Ok(Value::Bool(true)),
                    "false" => Ok(Value::Bool(false)),
                    _ => Err(Error::InvalidQuery(
                        "Cannot parse JSON string as BOOL".into(),
                    )),
                },
                _ => Err(Error::InvalidQuery("Cannot convert JSON to BOOL".into())),
            },
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                _ => Err(Error::InvalidQuery("Cannot parse string as BOOL".into())),
            },
            _ => Err(Error::InvalidQuery("BOOL requires a JSON argument".into())),
        }
    }

    fn fn_string_from_json(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("STRING requires 1 argument".into()));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(json) => match json {
                serde_json::Value::String(s) => Ok(Value::String(s.clone())),
                serde_json::Value::Number(n) => Ok(Value::String(n.to_string())),
                serde_json::Value::Bool(b) => Ok(Value::String(b.to_string())),
                _ => Ok(Value::String(json.to_string())),
            },
            Value::String(s) => Ok(Value::String(s.clone())),
            Value::Int64(n) => Ok(Value::String(n.to_string())),
            Value::Float64(f) => Ok(Value::String(f.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            other => Ok(Value::String(format!("{}", other))),
        }
    }

    fn fn_make_interval(&self, args: &[Value]) -> Result<Value> {
        use yachtsql_common::types::IntervalValue;

        let mut years = 0i32;
        let mut months = 0i32;
        let mut days = 0i32;
        let mut hours = 0i64;
        let mut minutes = 0i64;
        let mut seconds = 0i64;

        for (i, arg) in args.iter().enumerate() {
            let val = match arg {
                Value::Null => continue,
                Value::Int64(n) => *n,
                _ => {
                    return Err(Error::InvalidQuery(
                        "MAKE_INTERVAL arguments must be integers".into(),
                    ));
                }
            };
            match i {
                0 => years = val as i32,
                1 => months = val as i32,
                2 => days = val as i32,
                3 => hours = val,
                4 => minutes = val,
                5 => seconds = val,
                _ => {}
            }
        }

        let total_months = years * 12 + months;
        let total_nanos = (hours * 3600 + minutes * 60 + seconds) * 1_000_000_000;
        Ok(Value::Interval(IntervalValue {
            months: total_months,
            days,
            nanos: total_nanos,
        }))
    }

    fn fn_justify_days(&self, args: &[Value]) -> Result<Value> {
        use yachtsql_common::types::IntervalValue;
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "JUSTIFY_DAYS requires 1 argument".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Interval(iv) => {
                let extra_months = iv.days / 30;
                let remaining_days = iv.days % 30;
                Ok(Value::Interval(IntervalValue {
                    months: iv.months + extra_months,
                    days: remaining_days,
                    nanos: iv.nanos,
                }))
            }
            _ => Err(Error::InvalidQuery(
                "JUSTIFY_DAYS requires an interval argument".into(),
            )),
        }
    }

    fn fn_justify_hours(&self, args: &[Value]) -> Result<Value> {
        use yachtsql_common::types::IntervalValue;
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "JUSTIFY_HOURS requires 1 argument".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Interval(iv) => {
                const NANOS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000_000;
                let extra_days = (iv.nanos / NANOS_PER_DAY) as i32;
                let remaining_nanos = iv.nanos % NANOS_PER_DAY;
                Ok(Value::Interval(IntervalValue {
                    months: iv.months,
                    days: iv.days + extra_days,
                    nanos: remaining_nanos,
                }))
            }
            _ => Err(Error::InvalidQuery(
                "JUSTIFY_HOURS requires an interval argument".into(),
            )),
        }
    }

    fn fn_justify_interval(&self, args: &[Value]) -> Result<Value> {
        use yachtsql_common::types::IntervalValue;
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "JUSTIFY_INTERVAL requires 1 argument".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Interval(iv) => {
                const NANOS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000_000;
                let extra_days_from_nanos = (iv.nanos / NANOS_PER_DAY) as i32;
                let remaining_nanos = iv.nanos % NANOS_PER_DAY;
                let total_days = iv.days + extra_days_from_nanos;
                let extra_months_from_days = total_days / 30;
                let remaining_days = total_days % 30;
                Ok(Value::Interval(IntervalValue {
                    months: iv.months + extra_months_from_days,
                    days: remaining_days,
                    nanos: remaining_nanos,
                }))
            }
            _ => Err(Error::InvalidQuery(
                "JUSTIFY_INTERVAL requires an interval argument".into(),
            )),
        }
    }

    fn fn_safe_array_access(&self, args: &[Value], one_based: bool) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "SAFE_OFFSET/SAFE_ORDINAL requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Array(arr), Value::Int64(idx)) => {
                let index = if one_based { *idx - 1 } else { *idx };
                if index < 0 || index as usize >= arr.len() {
                    Ok(Value::Null)
                } else {
                    Ok(arr[index as usize].clone())
                }
            }
            _ => Err(Error::InvalidQuery(
                "SAFE_OFFSET/SAFE_ORDINAL requires array and integer arguments".into(),
            )),
        }
    }

    fn eval_custom_function(&self, name: &str, args: &[Value]) -> Result<Value> {
        if let Some(result) = self.try_eval_user_function(name, args)? {
            return Ok(result);
        }
        match name.to_uppercase().as_str() {
            "ST_GEOGFROMTEXT" | "ST_GEOGRAPHYFROMTEXT" => self.fn_st_geogfromtext(args),
            "ST_GEOGPOINT" | "ST_GEOGRAPHYPOINT" => self.fn_st_geogpoint(args),
            "ST_GEOGFROMGEOJSON" => self.fn_st_geogfromgeojson(args),
            "ST_ASTEXT" => self.fn_st_astext(args),
            "ST_ASGEOJSON" => self.fn_st_asgeojson(args),
            "ST_ASBINARY" => self.fn_st_asbinary(args),
            "ST_X" => self.fn_st_x(args),
            "ST_Y" => self.fn_st_y(args),
            "ST_AREA" => self.fn_st_area(args),
            "ST_LENGTH" => self.fn_st_length(args),
            "ST_PERIMETER" => self.fn_st_perimeter(args),
            "ST_DISTANCE" => self.fn_st_distance(args),
            "ST_CENTROID" => self.fn_st_centroid(args),
            "ST_BUFFER" => self.fn_st_buffer(args),
            "ST_BOUNDINGBOX" => self.fn_st_boundingbox(args),
            "ST_CLOSESTPOINT" => self.fn_st_closestpoint(args),
            "ST_CONTAINS" => self.fn_st_contains(args),
            "ST_INTERSECTS" => self.fn_st_intersects(args),
            "ST_UNION" => self.fn_st_union(args),
            "ST_INTERSECTION" => self.fn_st_intersection(args),
            "ST_DIFFERENCE" => self.fn_st_difference(args),
            "ST_MAKELINE" => self.fn_st_makeline(args),
            "ST_MAKEPOLYGON" => self.fn_st_makepolygon(args),
            "ST_NUMPOINTS" => self.fn_st_numpoints(args),
            "ST_ISCLOSED" => self.fn_st_isclosed(args),
            "ST_ISEMPTY" => self.fn_st_isempty(args),
            "NET.IP_FROM_STRING" => self.fn_net_ip_from_string(args),
            "NET.SAFE_IP_FROM_STRING" => self.fn_net_safe_ip_from_string(args),
            "NET.IP_TO_STRING" => self.fn_net_ip_to_string(args),
            "NET.IPV4_FROM_INT64" => self.fn_net_ipv4_from_int64(args),
            "NET.IPV4_TO_INT64" => self.fn_net_ipv4_to_int64(args),
            "NET.HOST" => self.fn_net_host(args),
            "NET.PUBLIC_SUFFIX" => self.fn_net_public_suffix(args),
            "NET.REG_DOMAIN" => self.fn_net_reg_domain(args),
            "ST_GEOMETRYTYPE" => self.fn_st_geometrytype(args),
            "ST_DIMENSION" => self.fn_st_dimension(args),
            "ST_WITHIN" => self.fn_st_within(args),
            "ST_DWITHIN" => self.fn_st_dwithin(args),
            "ST_COVERS" => self.fn_st_covers(args),
            "ST_COVEREDBY" => self.fn_st_coveredby(args),
            "ST_TOUCHES" => self.fn_st_touches(args),
            "ST_DISJOINT" => self.fn_st_disjoint(args),
            "ST_EQUALS" => self.fn_st_equals(args),
            "ST_CONVEXHULL" => self.fn_st_convexhull(args),
            "ST_SIMPLIFY" => self.fn_st_simplify(args),
            "ST_SNAPTOGRID" => self.fn_st_snaptogrid(args),
            "ST_BOUNDARY" => self.fn_st_boundary(args),
            "ST_STARTPOINT" => self.fn_st_startpoint(args),
            "ST_ENDPOINT" => self.fn_st_endpoint(args),
            "ST_POINTN" => self.fn_st_pointn(args),
            "ST_ISCOLLECTION" => self.fn_st_iscollection(args),
            "ST_ISRING" => self.fn_st_isring(args),
            "ST_MAXDISTANCE" => self.fn_st_maxdistance(args),
            "ST_GEOHASH" => self.fn_st_geohash(args),
            "ST_GEOGPOINTFROMGEOHASH" => self.fn_st_geogpointfromgeohash(args),
            "ST_BUFFERWITHTOLERANCE" => self.fn_st_bufferwithtolerance(args),
            "TIMESTAMP_DIFF" => self.fn_timestamp_diff(args),
            "DATETIME_ADD" => self.fn_datetime_add(args),
            "DATETIME_SUB" => self.fn_datetime_sub(args),
            "DATETIME_DIFF" => self.fn_datetime_diff(args),
            "TIMESTAMP_ADD" => self.fn_timestamp_add(args),
            "TIMESTAMP_SUB" => self.fn_timestamp_sub(args),
            "TIME_ADD" => self.fn_time_add(args),
            "TIME_SUB" => self.fn_time_sub(args),
            "TIME_DIFF" => self.fn_time_diff(args),
            "ALL" => self.fn_all(args),
            "ANY" | "SOME" => self.fn_any(args),
            "OFFSET" => self.fn_offset(args),
            "ORDINAL" => self.fn_ordinal(args),
            "JSON_ARRAY" => self.fn_json_array(args),
            "JSON_OBJECT" => self.fn_json_object(args),
            "JSON_SET" => self.fn_json_set(args),
            "JSON_REMOVE" => self.fn_json_remove(args),
            "JSON_STRIP_NULLS" => self.fn_json_strip_nulls(args),
            "JSON_QUERY_ARRAY" => self.fn_json_query_array(args),
            "JSON_VALUE_ARRAY" => self.fn_json_value_array(args),
            "JSON_EXTRACT_STRING_ARRAY" => self.fn_json_extract_string_array(args),
            "JSON_KEYS" => self.fn_json_keys(args),
            "REGEXP_INSTR" => self.fn_regexp_instr(args),
            "NET.IP_IN_NET" => self.fn_net_ip_in_net(args),
            "NET.IP_IS_PRIVATE" => self.fn_net_ip_is_private(args),
            "NET.IP_TRUNC" => self.fn_net_ip_trunc(args),
            "NET.IP_NET_MASK" => self.fn_net_ip_net_mask(args),
            "NET.MAKE_NET" => self.fn_net_make_net(args),
            "RANGE_CONTAINS" => self.fn_range_contains(args),
            "RANGE_OVERLAPS" => self.fn_range_overlaps(args),
            "RANGE_START" => self.fn_range_start(args),
            "RANGE_END" => self.fn_range_end(args),
            "RANGE_INTERSECT" => self.fn_range_intersect(args),
            "GENERATE_RANGE_ARRAY" => self.fn_generate_range_array(args),
            "LAX_INT64" => self.fn_lax_int64(args),
            "LAX_FLOAT64" => self.fn_lax_float64(args),
            "LAX_BOOL" => self.fn_lax_bool(args),
            "LAX_STRING" => self.fn_lax_string(args),
            "SESSION_USER" | "CURRENT_USER" => Ok(Value::String("anonymous".to_string())),
            "NULLIFZERO" => self.fn_nullifzero(args),
            "REGEXP_SUBSTR" => self.fn_regexp_substr(args),
            "ARRAY_SLICE" => self.fn_array_slice(args),
            "ARRAYENUMERATE" => self.fn_array_enumerate(args),
            "HLL_COUNT_EXTRACT" => self.fn_hll_count_extract(args),
            "MAP" => self.fn_map(args),
            "MAPKEYS" => self.fn_map_keys(args),
            "MAPVALUES" => self.fn_map_values(args),
            "KEYS.NEW_KEYSET" => self.fn_keys_new_keyset(args),
            "KEYS.ADD_KEY_FROM_RAW_BYTES" => self.fn_keys_add_key_from_raw_bytes(args),
            "KEYS.KEYSET_CHAIN" => self.fn_keys_keyset_chain(args),
            "KEYS.KEYSET_FROM_JSON" => self.fn_keys_keyset_from_json(args),
            "KEYS.KEYSET_TO_JSON" => self.fn_keys_keyset_to_json(args),
            "KEYS.KEYSET_LENGTH" => self.fn_keys_keyset_length(args),
            "KEYS.ROTATE_KEYSET" => self.fn_keys_rotate_keyset(args),
            "AEAD.ENCRYPT" => self.fn_aead_encrypt(args),
            "AEAD.DECRYPT_STRING" => self.fn_aead_decrypt_string(args),
            "AEAD.DECRYPT_BYTES" => self.fn_aead_decrypt_bytes(args),
            "DETERMINISTIC_ENCRYPT" => self.fn_deterministic_encrypt(args),
            "DETERMINISTIC_DECRYPT_STRING" => self.fn_deterministic_decrypt_string(args),
            "DETERMINISTIC_DECRYPT_BYTES" => self.fn_deterministic_decrypt_bytes(args),
            "COLLATE" => self.fn_collate(args),
            _ => Err(Error::UnsupportedFeature(format!(
                "Scalar function Custom(\"{}\") not yet implemented in IR evaluator",
                name
            ))),
        }
    }

    fn fn_collate(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::InvalidQuery(
                "COLLATE requires exactly 2 arguments".into(),
            ));
        }
        let Value::String(s) = &args[0] else {
            return Ok(Value::null());
        };
        let Value::String(collation) = &args[1] else {
            return Err(Error::InvalidQuery(
                "COLLATE second argument must be a string".into(),
            ));
        };
        let collation_lower = collation.to_lowercase();
        if collation_lower.contains(":ci") {
            Ok(Value::String(s.to_lowercase()))
        } else {
            Ok(Value::String(s.clone()))
        }
    }

    fn fn_map(&self, args: &[Value]) -> Result<Value> {
        if !args.len().is_multiple_of(2) {
            return Err(Error::InvalidQuery(
                "MAP requires an even number of arguments (alternating key, value pairs)".into(),
            ));
        }
        if args.is_empty() {
            return Ok(Value::Array(vec![]));
        }
        let map_entries: Vec<Value> = args
            .chunks(2)
            .map(|pair| {
                Value::Struct(vec![
                    ("key".to_string(), pair[0].clone()),
                    ("value".to_string(), pair[1].clone()),
                ])
            })
            .collect();
        Ok(Value::Array(map_entries))
    }

    fn fn_map_keys(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "MAPKEYS requires exactly 1 argument".into(),
            ));
        }
        let map_array = match &args[0] {
            Value::Array(arr) => arr,
            Value::Null => return Ok(Value::Null),
            _ => {
                return Err(Error::InvalidQuery(
                    "MAPKEYS argument must be a MAP (array of key-value structs)".into(),
                ));
            }
        };
        let keys: Vec<Value> = map_array
            .iter()
            .filter_map(|entry| {
                if let Value::Struct(fields) = entry {
                    fields
                        .iter()
                        .find(|(name, _)| name == "key")
                        .map(|(_, v)| v.clone())
                } else {
                    None
                }
            })
            .collect();
        Ok(Value::Array(keys))
    }

    fn fn_map_values(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "MAPVALUES requires exactly 1 argument".into(),
            ));
        }
        let map_array = match &args[0] {
            Value::Array(arr) => arr,
            Value::Null => return Ok(Value::Null),
            _ => {
                return Err(Error::InvalidQuery(
                    "MAPVALUES argument must be a MAP (array of key-value structs)".into(),
                ));
            }
        };
        let values: Vec<Value> = map_array
            .iter()
            .filter_map(|entry| {
                if let Value::Struct(fields) = entry {
                    fields
                        .iter()
                        .find(|(name, _)| name == "value")
                        .map(|(_, v)| v.clone())
                } else {
                    None
                }
            })
            .collect();
        Ok(Value::Array(values))
    }

    fn fn_timestamp_diff(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "TIMESTAMP_DIFF requires timestamp1, timestamp2, and date_part".into(),
            ));
        }
        let date_part = args[2].as_str().unwrap_or("SECOND").to_uppercase();
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Timestamp(ts1), Value::Timestamp(ts2)) => {
                let diff_micros = ts1.timestamp_micros() - ts2.timestamp_micros();
                let result = match date_part.as_str() {
                    "MICROSECOND" => diff_micros,
                    "MILLISECOND" => diff_micros / 1_000,
                    "SECOND" => diff_micros / 1_000_000,
                    "MINUTE" => diff_micros / 60_000_000,
                    "HOUR" => diff_micros / 3_600_000_000,
                    "DAY" => diff_micros / 86_400_000_000,
                    _ => diff_micros / 1_000_000,
                };
                Ok(Value::Int64(result))
            }
            (Value::DateTime(dt1), Value::DateTime(dt2)) => {
                let diff_micros =
                    dt1.and_utc().timestamp_micros() - dt2.and_utc().timestamp_micros();
                let result = match date_part.as_str() {
                    "MICROSECOND" => diff_micros,
                    "MILLISECOND" => diff_micros / 1_000,
                    "SECOND" => diff_micros / 1_000_000,
                    "MINUTE" => diff_micros / 60_000_000,
                    "HOUR" => diff_micros / 3_600_000_000,
                    "DAY" => diff_micros / 86_400_000_000,
                    _ => diff_micros / 1_000_000,
                };
                Ok(Value::Int64(result))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_DIFF expects timestamp arguments".into(),
            )),
        }
    }

    fn fn_datetime_add(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "DATETIME_ADD requires datetime and interval arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::DateTime(dt), Value::Interval(iv)) => {
                let new_dt = add_interval_to_datetime(dt, iv)?;
                Ok(Value::DateTime(new_dt))
            }
            _ => Err(Error::InvalidQuery(
                "DATETIME_ADD expects datetime and interval arguments".into(),
            )),
        }
    }

    fn fn_datetime_sub(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "DATETIME_SUB requires datetime and interval arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::DateTime(dt), Value::Interval(iv)) => {
                let neg_iv = negate_interval(iv);
                let new_dt = add_interval_to_datetime(dt, &neg_iv)?;
                Ok(Value::DateTime(new_dt))
            }
            _ => Err(Error::InvalidQuery(
                "DATETIME_SUB expects datetime and interval arguments".into(),
            )),
        }
    }

    fn fn_datetime_diff(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "DATETIME_DIFF requires datetime1, datetime2, and date_part".into(),
            ));
        }
        let date_part = args[2].as_str().unwrap_or("SECOND").to_uppercase();
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::DateTime(dt1), Value::DateTime(dt2)) => {
                let diff_micros =
                    dt1.and_utc().timestamp_micros() - dt2.and_utc().timestamp_micros();
                let result = match date_part.as_str() {
                    "MICROSECOND" => diff_micros,
                    "MILLISECOND" => diff_micros / 1_000,
                    "SECOND" => diff_micros / 1_000_000,
                    "MINUTE" => diff_micros / 60_000_000,
                    "HOUR" => diff_micros / 3_600_000_000,
                    "DAY" => diff_micros / 86_400_000_000,
                    _ => diff_micros / 1_000_000,
                };
                Ok(Value::Int64(result))
            }
            _ => Err(Error::InvalidQuery(
                "DATETIME_DIFF expects datetime arguments".into(),
            )),
        }
    }

    fn fn_timestamp_add(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIMESTAMP_ADD requires timestamp and interval arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Timestamp(ts), Value::Interval(iv)) => {
                let nanos_to_add = iv.nanos
                    + iv.days as i64 * 86_400_000_000_000
                    + iv.months as i64 * 30 * 86_400_000_000_000;
                let new_ts = *ts + chrono::Duration::nanoseconds(nanos_to_add);
                Ok(Value::Timestamp(new_ts))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_ADD expects timestamp and interval arguments".into(),
            )),
        }
    }

    fn fn_timestamp_sub(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIMESTAMP_SUB requires timestamp and interval arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Timestamp(ts), Value::Interval(iv)) => {
                let nanos_to_sub = iv.nanos
                    + iv.days as i64 * 86_400_000_000_000
                    + iv.months as i64 * 30 * 86_400_000_000_000;
                let new_ts = *ts - chrono::Duration::nanoseconds(nanos_to_sub);
                Ok(Value::Timestamp(new_ts))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_SUB expects timestamp and interval arguments".into(),
            )),
        }
    }

    fn fn_time_add(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIME_ADD requires time and interval arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Time(t), Value::Interval(iv)) => {
                let nanos_to_add = iv.nanos;
                let total_nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                    + t.nanosecond() as i64
                    + nanos_to_add;
                let total_nanos = total_nanos.rem_euclid(86_400_000_000_000);
                let secs = (total_nanos / 1_000_000_000) as u32;
                let nanos = (total_nanos % 1_000_000_000) as u32;
                let new_time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                    .unwrap_or(*t);
                Ok(Value::Time(new_time))
            }
            _ => Err(Error::InvalidQuery(
                "TIME_ADD expects time and interval arguments".into(),
            )),
        }
    }

    fn fn_time_sub(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIME_SUB requires time and interval arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Time(t), Value::Interval(iv)) => {
                let nanos_to_sub = iv.nanos;
                let total_nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                    + t.nanosecond() as i64
                    - nanos_to_sub;
                let total_nanos = total_nanos.rem_euclid(86_400_000_000_000);
                let secs = (total_nanos / 1_000_000_000) as u32;
                let nanos = (total_nanos % 1_000_000_000) as u32;
                let new_time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                    .unwrap_or(*t);
                Ok(Value::Time(new_time))
            }
            _ => Err(Error::InvalidQuery(
                "TIME_SUB expects time and interval arguments".into(),
            )),
        }
    }

    fn fn_time_diff(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "TIME_DIFF requires time1, time2, and date_part arguments".into(),
            ));
        }
        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) => Ok(Value::Null),
            (Value::Time(t1), Value::Time(t2), Value::String(part)) => {
                let t1_nanos =
                    t1.num_seconds_from_midnight() as i64 * 1_000_000_000 + t1.nanosecond() as i64;
                let t2_nanos =
                    t2.num_seconds_from_midnight() as i64 * 1_000_000_000 + t2.nanosecond() as i64;
                let diff_nanos = t1_nanos - t2_nanos;
                let result = match part.to_uppercase().as_str() {
                    "HOUR" => diff_nanos / 3_600_000_000_000,
                    "MINUTE" => diff_nanos / 60_000_000_000,
                    "SECOND" => diff_nanos / 1_000_000_000,
                    "MILLISECOND" => diff_nanos / 1_000_000,
                    "MICROSECOND" => diff_nanos / 1_000,
                    "NANOSECOND" => diff_nanos,
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "Invalid date part for TIME_DIFF: {}",
                            part
                        )));
                    }
                };
                Ok(Value::Int64(result))
            }
            _ => Err(Error::InvalidQuery(
                "TIME_DIFF expects time arguments".into(),
            )),
        }
    }

    fn fn_all(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Bool(true));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Array(arr) => {
                for v in arr {
                    match v {
                        Value::Null => return Ok(Value::Null),
                        Value::Bool(false) => return Ok(Value::Bool(false)),
                        Value::Bool(true) => {}
                        _ => return Ok(Value::Bool(false)),
                    }
                }
                Ok(Value::Bool(true))
            }
            Value::Bool(b) => Ok(Value::Bool(*b)),
            _ => Ok(Value::Bool(true)),
        }
    }

    fn fn_any(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Bool(false));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Array(arr) => {
                let mut has_null = false;
                for v in arr {
                    match v {
                        Value::Null => has_null = true,
                        Value::Bool(true) => return Ok(Value::Bool(true)),
                        _ => {}
                    }
                }
                if has_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(false))
                }
            }
            Value::Bool(b) => Ok(Value::Bool(*b)),
            _ => Ok(Value::Bool(false)),
        }
    }

    fn fn_range_contains(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "RANGE_CONTAINS requires range and value arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Range(r), val) => {
                let in_range = match (&r.start, &r.end) {
                    (Some(start), Some(end)) => val >= start && val < end,
                    (Some(start), None) => val >= start,
                    (None, Some(end)) => val < end,
                    (None, None) => true,
                };
                Ok(Value::Bool(in_range))
            }
            _ => Ok(Value::Bool(false)),
        }
    }

    fn fn_range_overlaps(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "RANGE_OVERLAPS requires two range arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Range(r1), Value::Range(r2)) => {
                let (start1, end1) = (r1.start.as_deref(), r1.end.as_deref());
                let (start2, end2) = (r2.start.as_deref(), r2.end.as_deref());
                let overlaps = match (start1, end1, start2, end2) {
                    (Some(s1), Some(e1), Some(s2), Some(e2)) => {
                        self.value_less_than(s1, e2) && self.value_less_than(s2, e1)
                    }
                    (None, Some(e1), Some(s2), _) => self.value_less_than(s2, e1),
                    (Some(s1), None, _, Some(e2)) => self.value_less_than(s1, e2),
                    (None, _, _, None) | (_, None, None, _) => true,
                    _ => false,
                };
                Ok(Value::Bool(overlaps))
            }
            _ => Err(Error::InvalidQuery(
                "RANGE_OVERLAPS expects range arguments".into(),
            )),
        }
    }

    fn value_less_than(&self, a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Int64(x), Value::Int64(y)) => x < y,
            (Value::Float64(x), Value::Float64(y)) => x < y,
            (Value::Date(x), Value::Date(y)) => x < y,
            (Value::DateTime(x), Value::DateTime(y)) => x < y,
            (Value::Timestamp(x), Value::Timestamp(y)) => x < y,
            (Value::Time(x), Value::Time(y)) => x < y,
            (Value::String(x), Value::String(y)) => x < y,
            _ => false,
        }
    }

    fn fn_range_start(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Range(range) => match &range.start {
                Some(v) => Ok((**v).clone()),
                None => Ok(Value::Null),
            },
            _ => Err(Error::InvalidQuery(
                "RANGE_START expects a range argument".into(),
            )),
        }
    }

    fn fn_range_end(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Range(range) => match &range.end {
                Some(v) => Ok((**v).clone()),
                None => Ok(Value::Null),
            },
            _ => Err(Error::InvalidQuery(
                "RANGE_END expects a range argument".into(),
            )),
        }
    }

    fn fn_range_intersect(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "RANGE_INTERSECT requires two range arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Range(r1), Value::Range(r2)) => {
                use yachtsql_common::types::RangeValue;
                let start = match (&r1.start, &r2.start) {
                    (Some(a), Some(b)) if **a > **b => Some(Box::new((**a).clone())),
                    (_, Some(b)) => Some(Box::new((**b).clone())),
                    (Some(a), None) => Some(Box::new((**a).clone())),
                    (None, None) => None,
                };
                let end = match (&r1.end, &r2.end) {
                    (Some(a), Some(b)) if **a < **b => Some(Box::new((**a).clone())),
                    (_, Some(b)) => Some(Box::new((**b).clone())),
                    (Some(a), None) => Some(Box::new((**a).clone())),
                    (None, None) => None,
                };
                if let (Some(s), Some(e)) = (&start, &end) {
                    if **s > **e {
                        return Ok(Value::Null);
                    }
                }
                Ok(Value::Range(RangeValue { start, end }))
            }
            _ => Err(Error::InvalidQuery(
                "RANGE_INTERSECT expects range arguments".into(),
            )),
        }
    }

    fn fn_generate_range_array(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "GENERATE_RANGE_ARRAY requires at least 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Range(range), Value::Interval(interval)) => {
                use yachtsql_common::types::RangeValue;
                let mut results = vec![];
                let start = range.start.as_deref();
                let end = range.end.as_deref();
                match (start, end) {
                    (Some(Value::Date(start_date)), Some(Value::Date(end_date))) => {
                        let mut current = *start_date;
                        while current < *end_date {
                            let mut next = current;
                            if interval.days != 0 {
                                next += chrono::Duration::days(interval.days as i64);
                            }
                            if interval.months != 0 {
                                let year = next.year();
                                let month = next.month() as i32 + interval.months;
                                let new_year = year + (month - 1) / 12;
                                let new_month = ((month - 1) % 12 + 12) % 12 + 1;
                                next = NaiveDate::from_ymd_opt(
                                    new_year,
                                    new_month as u32,
                                    next.day().min(28),
                                )
                                .unwrap_or(next);
                            }
                            let range_end = if next > *end_date { *end_date } else { next };
                            results.push(Value::Range(RangeValue {
                                start: Some(Box::new(Value::Date(current))),
                                end: Some(Box::new(Value::Date(range_end))),
                            }));
                            current = next;
                        }
                    }
                    (Some(Value::Int64(start_val)), Some(Value::Int64(end_val))) => {
                        let step = if interval.days != 0 {
                            interval.days as i64
                        } else {
                            1
                        };
                        let mut current = *start_val;
                        while current < *end_val {
                            let next = current + step;
                            let range_end = if next > *end_val { *end_val } else { next };
                            results.push(Value::Range(RangeValue {
                                start: Some(Box::new(Value::Int64(current))),
                                end: Some(Box::new(Value::Int64(range_end))),
                            }));
                            current = next;
                        }
                    }
                    _ => return Ok(Value::Array(vec![])),
                }
                Ok(Value::Array(results))
            }
            _ => Ok(Value::Array(vec![])),
        }
    }

    fn fn_lax_int64(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Int64(n) => Ok(Value::Int64(*n)),
            Value::Float64(f) => Ok(Value::Int64(f.0 as i64)),
            Value::Bool(b) => Ok(Value::Int64(if *b { 1 } else { 0 })),
            Value::String(s) => {
                if let Ok(n) = s.trim().parse::<i64>() {
                    Ok(Value::Int64(n))
                } else {
                    Ok(Value::Null)
                }
            }
            Value::Json(j) => {
                if let Some(n) = j.as_i64() {
                    Ok(Value::Int64(n))
                } else if let Some(f) = j.as_f64() {
                    Ok(Value::Int64(f as i64))
                } else if let Some(s) = j.as_str() {
                    if let Ok(n) = s.trim().parse::<i64>() {
                        Ok(Value::Int64(n))
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Ok(Value::Null),
        }
    }

    fn fn_lax_float64(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Float64(f) => Ok(Value::Float64(*f)),
            Value::Int64(n) => Ok(Value::Float64(OrderedFloat(*n as f64))),
            Value::Bool(b) => Ok(Value::Float64(OrderedFloat(if *b { 1.0 } else { 0.0 }))),
            Value::String(s) => {
                if let Ok(f) = s.trim().parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(f)))
                } else {
                    Ok(Value::Null)
                }
            }
            Value::Json(j) => {
                if let Some(f) = j.as_f64() {
                    Ok(Value::Float64(OrderedFloat(f)))
                } else if let Some(s) = j.as_str() {
                    if let Ok(f) = s.trim().parse::<f64>() {
                        Ok(Value::Float64(OrderedFloat(f)))
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Ok(Value::Null),
        }
    }

    fn fn_lax_bool(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::Int64(n) => Ok(Value::Bool(*n != 0)),
            Value::Float64(f) => Ok(Value::Bool(f.0 != 0.0)),
            Value::String(s) => {
                let lower = s.trim().to_lowercase();
                if lower == "true" || lower == "1" {
                    Ok(Value::Bool(true))
                } else if lower == "false" || lower == "0" {
                    Ok(Value::Bool(false))
                } else {
                    Ok(Value::Null)
                }
            }
            Value::Json(j) => {
                if let Some(b) = j.as_bool() {
                    Ok(Value::Bool(b))
                } else if let Some(s) = j.as_str() {
                    let lower = s.trim().to_lowercase();
                    if lower == "true" || lower == "1" {
                        Ok(Value::Bool(true))
                    } else if lower == "false" || lower == "0" {
                        Ok(Value::Bool(false))
                    } else {
                        Ok(Value::Null)
                    }
                } else if let Some(n) = j.as_i64() {
                    Ok(Value::Bool(n != 0))
                } else if let Some(n) = j.as_f64() {
                    Ok(Value::Bool(n != 0.0))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Ok(Value::Null),
        }
    }

    fn fn_lax_string(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(s) => Ok(Value::String(s.clone())),
            Value::Int64(n) => Ok(Value::String(n.to_string())),
            Value::Float64(f) => Ok(Value::String(f.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            Value::Json(j) => {
                if let Some(s) = j.as_str() {
                    Ok(Value::String(s.to_string()))
                } else {
                    Ok(Value::String(j.to_string()))
                }
            }
            _ => Ok(Value::Null),
        }
    }

    fn fn_nullifzero(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Int64(0) => Ok(Value::Null),
            Value::Int64(n) => Ok(Value::Int64(*n)),
            Value::Float64(f) if f.0 == 0.0 => Ok(Value::Null),
            Value::Float64(f) => Ok(Value::Float64(*f)),
            v => Ok(v.clone()),
        }
    }

    fn try_eval_user_function(&self, name: &str, args: &[Value]) -> Result<Option<Value>> {
        let user_functions = match self.user_functions {
            Some(funcs) => funcs,
            None => return Ok(None),
        };

        let upper_name = name.to_uppercase();
        let udf = match user_functions.get(&upper_name) {
            Some(f) => f,
            None => return Ok(None),
        };

        if args.len() != udf.parameters.len() {
            return Err(Error::InvalidQuery(format!(
                "Function {} expects {} arguments, got {}",
                name,
                udf.parameters.len(),
                args.len()
            )));
        }

        match &udf.body {
            FunctionBody::Sql(body_expr) => {
                let substituted_body = self.substitute_udf_params(body_expr, &udf.parameters, args);
                Ok(Some(self.evaluate(&substituted_body, &Record::new())?))
            }
            FunctionBody::JavaScript(js_code) => {
                let param_names: Vec<String> =
                    udf.parameters.iter().map(|p| p.name.clone()).collect();
                let result = crate::js_udf::evaluate_js_function(js_code, &param_names, args)
                    .map_err(|e| Error::InvalidQuery(format!("JavaScript UDF error: {}", e)))?;
                Ok(Some(result))
            }
            FunctionBody::Language { name: lang, code } => {
                if lang.to_uppercase() == "PYTHON" {
                    let param_names: Vec<String> =
                        udf.parameters.iter().map(|p| p.name.clone()).collect();
                    let result = crate::py_udf::evaluate_py_function(code, &param_names, args)
                        .map_err(|e| Error::InvalidQuery(format!("Python UDF error: {}", e)))?;
                    Ok(Some(result))
                } else {
                    Err(Error::UnsupportedFeature(format!(
                        "Language {} is not supported for UDFs",
                        lang
                    )))
                }
            }
            FunctionBody::SqlQuery(_) => Err(Error::InvalidQuery(
                "Table functions cannot be called as scalar functions".to_string(),
            )),
        }
    }

    fn substitute_udf_params(&self, expr: &Expr, params: &[FunctionArg], args: &[Value]) -> Expr {
        match expr {
            Expr::Column { name, .. } => {
                let col_upper = name.to_uppercase();
                for (i, param) in params.iter().enumerate() {
                    if param.name.to_uppercase() == col_upper {
                        return self.value_to_literal_expr(&args[i]);
                    }
                }
                expr.clone()
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(self.substitute_udf_params(left, params, args)),
                op: *op,
                right: Box::new(self.substitute_udf_params(right, params, args)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.substitute_udf_params(inner, params, args)),
            },
            Expr::ScalarFunction {
                name,
                args: fn_args,
            } => Expr::ScalarFunction {
                name: name.clone(),
                args: fn_args
                    .iter()
                    .map(|a| self.substitute_udf_params(a, params, args))
                    .collect(),
            },
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|o| Box::new(self.substitute_udf_params(o, params, args))),
                when_clauses: when_clauses
                    .iter()
                    .map(|wc| WhenClause {
                        condition: self.substitute_udf_params(&wc.condition, params, args),
                        result: self.substitute_udf_params(&wc.result, params, args),
                    })
                    .collect(),
                else_result: else_result
                    .as_ref()
                    .map(|e| Box::new(self.substitute_udf_params(e, params, args))),
            },
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => Expr::Cast {
                expr: Box::new(self.substitute_udf_params(inner, params, args)),
                data_type: data_type.clone(),
                safe: *safe,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(self.substitute_udf_params(expr, params, args)),
                list: list
                    .iter()
                    .map(|l| self.substitute_udf_params(l, params, args))
                    .collect(),
                negated: *negated,
            },
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => Expr::Between {
                expr: Box::new(self.substitute_udf_params(expr, params, args)),
                low: Box::new(self.substitute_udf_params(low, params, args)),
                high: Box::new(self.substitute_udf_params(high, params, args)),
                negated: *negated,
            },
            _ => expr.clone(),
        }
    }

    fn value_to_literal_expr(&self, value: &Value) -> Expr {
        match value {
            Value::Null => Expr::Literal(Literal::Null),
            Value::Bool(b) => Expr::Literal(Literal::Bool(*b)),
            Value::Int64(n) => Expr::Literal(Literal::Int64(*n)),
            Value::Float64(f) => Expr::Literal(Literal::Float64(OrderedFloat(f.0))),
            Value::String(s) => Expr::Literal(Literal::String(s.clone())),
            Value::Numeric(d) => Expr::Literal(Literal::Numeric(*d)),
            Value::Date(d) => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = d.signed_duration_since(epoch).num_days() as i32;
                Expr::Literal(Literal::Date(days))
            }
            Value::Time(t) => {
                let nanos =
                    t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64;
                Expr::Literal(Literal::Time(nanos))
            }
            Value::Timestamp(ts) => {
                let micros = ts.timestamp_micros();
                Expr::Literal(Literal::Timestamp(micros))
            }
            Value::DateTime(dt) => {
                let micros = dt.and_utc().timestamp_micros();
                Expr::Literal(Literal::Datetime(micros))
            }
            _ => Expr::Literal(Literal::Null),
        }
    }

    fn fn_hll_count_extract(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "HLL_COUNT_EXTRACT requires 1 argument".to_string(),
            ));
        }
        if args[0].is_null() {
            return Ok(Value::Null);
        }
        let sketch_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: args[0].data_type().to_string(),
        })?;
        if let Some(encoded) = sketch_str.strip_prefix("HLL_SKETCH:p14:") {
            if let Ok(registers) =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
            {
                let m = registers.len() as f64;
                let mut sum: f64 = 0.0;
                let mut zeros = 0i32;
                for &r in &registers {
                    sum += 2.0_f64.powf(-(r as f64));
                    if r == 0 {
                        zeros += 1;
                    }
                }
                let alpha_m = 0.7213 / (1.0 + 1.079 / m);
                let raw_est = alpha_m * m * m / sum;
                let estimate = if raw_est <= 2.5 * m && zeros > 0 {
                    m * (m / zeros as f64).ln()
                } else {
                    raw_est
                };
                return Ok(Value::Int64(estimate.round() as i64));
            }
        } else if let Some(rest) = sketch_str.strip_prefix("HLL_SKETCH:p15:n") {
            if let Ok(count) = rest.parse::<i64>() {
                return Ok(Value::Int64(count));
            }
        }
        Ok(Value::Int64(0))
    }

    fn fn_deterministic_encrypt(&self, args: &[Value]) -> Result<Value> {
        self.fn_aead_encrypt(args)
    }

    fn fn_deterministic_decrypt_string(&self, args: &[Value]) -> Result<Value> {
        self.fn_aead_decrypt_string(args)
    }

    fn fn_deterministic_decrypt_bytes(&self, args: &[Value]) -> Result<Value> {
        self.fn_aead_decrypt_bytes(args)
    }
}
