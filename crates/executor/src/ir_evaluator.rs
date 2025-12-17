#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::collapsible_if)]

use std::collections::HashMap;

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike, Utc};
use ordered_float::OrderedFloat;
use rust_decimal::prelude::ToPrimitive;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, IntervalValue, Value};
use yachtsql_ir::{
    AggregateFunction, BinaryOp, DateTimeField, Expr, Literal, ScalarFunction, TrimWhere, UnaryOp,
    WhenClause,
};
use yachtsql_storage::{Record, Schema};

pub struct IrEvaluator<'a> {
    schema: &'a Schema,
    variables: Option<&'a HashMap<String, Value>>,
}

impl<'a> IrEvaluator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            variables: None,
        }
    }

    pub fn with_variables(mut self, variables: &'a HashMap<String, Value>) -> Self {
        self.variables = Some(variables);
        self
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
            Expr::Cast { expr, data_type } => self.eval_cast(expr, data_type, record),
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
            Expr::Array { elements } => self.eval_array(elements, record),
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
        }
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
        }

        let idx = if let Some(tbl) = table {
            let qualified = format!("{}.{}", tbl, name);
            self.schema
                .field_index(&qualified)
                .or_else(|| self.schema.field_index(name))
        } else {
            self.schema.field_index(name)
        };

        match idx {
            Some(i) if i < record.values().len() => Ok(record.values()[i].clone()),
            _ => Err(Error::ColumnNotFound(name.to_string())),
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
            BinaryOp::Eq => Ok(Value::Bool(self.values_equal(&left_val, &right_val))),
            BinaryOp::NotEq => Ok(Value::Bool(!self.values_equal(&left_val, &right_val))),
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
            ScalarFunction::IfNull => self.fn_ifnull(&arg_values),
            ScalarFunction::NullIf => self.fn_nullif(&arg_values),
            ScalarFunction::If => self.fn_if(&arg_values),
            ScalarFunction::Abs => self.fn_abs(&arg_values),
            ScalarFunction::Floor => self.fn_floor(&arg_values),
            ScalarFunction::Ceil => self.fn_ceil(&arg_values),
            ScalarFunction::Round => self.fn_round(&arg_values),
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

    fn eval_cast(&self, expr: &Expr, _target_type: &DataType, record: &Record) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        Ok(val)
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
                let start_idx = start_val
                    .and_then(|v| v.as_i64())
                    .map(|n| (n - 1).max(0) as usize)
                    .unwrap_or(0);
                let chars: Vec<char> = s.chars().collect();
                let len = len_val
                    .and_then(|v| v.as_i64())
                    .map(|n| n.max(0) as usize)
                    .unwrap_or(chars.len().saturating_sub(start_idx));
                let result: String = chars.into_iter().skip(start_idx).take(len).collect();
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery(
                "SUBSTRING requires string argument".into(),
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
        let index_val = self.evaluate(index, record)?;

        match (&array_val, &index_val) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Array(elements), Value::Int64(idx)) => {
                let actual_idx = if *idx < 0 {
                    (elements.len() as i64 + idx) as usize
                } else {
                    *idx as usize
                };
                Ok(elements.get(actual_idx).cloned().unwrap_or(Value::Null))
            }
            _ => Err(Error::InvalidQuery(
                "Array access requires array and integer index".into(),
            )),
        }
    }

    fn eval_struct(&self, fields: &[(Option<String>, Expr)], record: &Record) -> Result<Value> {
        let struct_fields: Result<Vec<_>> = fields
            .iter()
            .enumerate()
            .map(|(i, (name, expr))| {
                let val = self.evaluate(expr, record)?;
                let field_name = name.clone().unwrap_or_else(|| format!("_{}", i));
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
            _ => Ok(Value::String(value.to_string())),
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
            (Value::Date(a), Value::Date(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Timestamp(a), Value::Timestamp(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
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

    fn fn_upper(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::String(s.to_uppercase())),
            _ => Err(Error::InvalidQuery("UPPER requires string argument".into())),
        }
    }

    fn fn_lower(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::String(s.to_lowercase())),
            _ => Err(Error::InvalidQuery("LOWER requires string argument".into())),
        }
    }

    fn fn_length(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::Int64(s.chars().count() as i64)),
            Some(Value::Bytes(b)) => Ok(Value::Int64(b.len() as i64)),
            Some(Value::Array(a)) => Ok(Value::Int64(a.len() as i64)),
            _ => Err(Error::InvalidQuery(
                "LENGTH requires string, bytes, or array argument".into(),
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

    fn fn_abs(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Int64(n.abs())),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.abs()))),
            Some(Value::Numeric(d)) => Ok(Value::Numeric(d.abs())),
            _ => Err(Error::InvalidQuery("ABS requires numeric argument".into())),
        }
    }

    fn fn_floor(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Int64(*n)),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.floor()))),
            Some(Value::Numeric(d)) => Ok(Value::Numeric(d.floor())),
            _ => Err(Error::InvalidQuery(
                "FLOOR requires numeric argument".into(),
            )),
        }
    }

    fn fn_ceil(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Int64(*n)),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.ceil()))),
            Some(Value::Numeric(d)) => Ok(Value::Numeric(d.ceil())),
            _ => Err(Error::InvalidQuery("CEIL requires numeric argument".into())),
        }
    }

    fn fn_round(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "ROUND requires at least 1 argument".into(),
            ));
        }
        let precision = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Int64(n) => Ok(Value::Int64(*n)),
            Value::Float64(f) => {
                let mult = 10f64.powi(precision as i32);
                Ok(Value::Float64(OrderedFloat((f.0 * mult).round() / mult)))
            }
            Value::Numeric(d) => Ok(Value::Numeric(d.round_dp(precision.max(0) as u32))),
            _ => Err(Error::InvalidQuery(
                "ROUND requires numeric argument".into(),
            )),
        }
    }
}

fn like_pattern_to_regex(pattern: &str) -> String {
    let mut regex = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' => {
                if let Some(&next) = chars.peek() {
                    regex.push_str(&regex::escape(&next.to_string()));
                    chars.next();
                }
            }
            _ => regex.push_str(&regex::escape(&c.to_string())),
        }
    }
    regex
}

fn extract_datetime_field(val: &Value, field: DateTimeField) -> Result<Value> {
    match val {
        Value::Null => Ok(Value::Null),
        Value::Date(date) => extract_from_date(date, field),
        Value::Timestamp(dt) => extract_from_datetime(&dt.naive_utc(), field),
        Value::Time(time) => extract_from_time(time, field),
        _ => Err(Error::InvalidQuery(
            "EXTRACT requires date/time/timestamp argument".into(),
        )),
    }
}

fn extract_from_date(date: &NaiveDate, field: DateTimeField) -> Result<Value> {
    match field {
        DateTimeField::Year => Ok(Value::Int64(date.year() as i64)),
        DateTimeField::Month => Ok(Value::Int64(date.month() as i64)),
        DateTimeField::Day => Ok(Value::Int64(date.day() as i64)),
        DateTimeField::Week => Ok(Value::Int64(date.iso_week().week() as i64)),
        DateTimeField::DayOfWeek => Ok(Value::Int64(date.weekday().num_days_from_sunday() as i64)),
        DateTimeField::DayOfYear => Ok(Value::Int64(date.ordinal() as i64)),
        DateTimeField::Quarter => Ok(Value::Int64(((date.month() - 1) / 3 + 1) as i64)),
        _ => Err(Error::InvalidQuery(format!(
            "Cannot extract {:?} from date",
            field
        ))),
    }
}

fn extract_from_datetime(dt: &chrono::NaiveDateTime, field: DateTimeField) -> Result<Value> {
    match field {
        DateTimeField::Year => Ok(Value::Int64(dt.year() as i64)),
        DateTimeField::Month => Ok(Value::Int64(dt.month() as i64)),
        DateTimeField::Day => Ok(Value::Int64(dt.day() as i64)),
        DateTimeField::Hour => Ok(Value::Int64(dt.hour() as i64)),
        DateTimeField::Minute => Ok(Value::Int64(dt.minute() as i64)),
        DateTimeField::Second => Ok(Value::Int64(dt.second() as i64)),
        DateTimeField::Millisecond => Ok(Value::Int64((dt.nanosecond() / 1_000_000) as i64)),
        DateTimeField::Microsecond => Ok(Value::Int64((dt.nanosecond() / 1000) as i64)),
        DateTimeField::Nanosecond => Ok(Value::Int64(dt.nanosecond() as i64)),
        DateTimeField::Week => Ok(Value::Int64(dt.iso_week().week() as i64)),
        DateTimeField::DayOfWeek => Ok(Value::Int64(dt.weekday().num_days_from_sunday() as i64)),
        DateTimeField::DayOfYear => Ok(Value::Int64(dt.ordinal() as i64)),
        DateTimeField::Quarter => Ok(Value::Int64(((dt.month() - 1) / 3 + 1) as i64)),
        DateTimeField::Epoch => Ok(Value::Int64(dt.and_utc().timestamp())),
        _ => Err(Error::InvalidQuery(format!(
            "Cannot extract {:?} from timestamp",
            field
        ))),
    }
}

fn extract_from_time(time: &NaiveTime, field: DateTimeField) -> Result<Value> {
    match field {
        DateTimeField::Hour => Ok(Value::Int64(time.hour() as i64)),
        DateTimeField::Minute => Ok(Value::Int64(time.minute() as i64)),
        DateTimeField::Second => Ok(Value::Int64(time.second() as i64)),
        DateTimeField::Millisecond => Ok(Value::Int64((time.nanosecond() / 1_000_000) as i64)),
        DateTimeField::Microsecond => Ok(Value::Int64((time.nanosecond() / 1000) as i64)),
        DateTimeField::Nanosecond => Ok(Value::Int64(time.nanosecond() as i64)),
        _ => Err(Error::InvalidQuery(format!(
            "Cannot extract {:?} from time",
            field
        ))),
    }
}

fn parse_date_string(s: &str) -> Result<Value> {
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| Error::InvalidQuery(format!("Invalid date string: {}", e)))?;
    Ok(Value::Date(date))
}

fn parse_time_string(s: &str) -> Result<Value> {
    let time = NaiveTime::parse_from_str(s, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
        .map_err(|e| Error::InvalidQuery(format!("Invalid time string: {}", e)))?;
    Ok(Value::Time(time))
}

fn parse_timestamp_string(s: &str) -> Result<Value> {
    use chrono::NaiveDateTime;
    let dt = DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
                .map(|ndt| ndt.and_utc())
        })
        .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp string: {}", e)))?;
    Ok(Value::Timestamp(dt))
}

fn parse_interval_string(_s: &str) -> Result<Value> {
    Ok(Value::Interval(IntervalValue {
        months: 0,
        days: 0,
        nanos: 0,
    }))
}

fn interval_from_field(n: i64, field: DateTimeField) -> Result<Value> {
    match field {
        DateTimeField::Year => Ok(Value::Interval(IntervalValue {
            months: n as i32 * 12,
            days: 0,
            nanos: 0,
        })),
        DateTimeField::Month => Ok(Value::Interval(IntervalValue {
            months: n as i32,
            days: 0,
            nanos: 0,
        })),
        DateTimeField::Day => Ok(Value::Interval(IntervalValue {
            months: 0,
            days: n as i32,
            nanos: 0,
        })),
        DateTimeField::Hour => Ok(Value::Interval(IntervalValue {
            months: 0,
            days: 0,
            nanos: n * 3_600_000_000_000,
        })),
        DateTimeField::Minute => Ok(Value::Interval(IntervalValue {
            months: 0,
            days: 0,
            nanos: n * 60_000_000_000,
        })),
        DateTimeField::Second => Ok(Value::Interval(IntervalValue {
            months: 0,
            days: 0,
            nanos: n * 1_000_000_000,
        })),
        _ => Err(Error::InvalidQuery(format!(
            "Invalid interval field: {:?}",
            field
        ))),
    }
}
