use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::super::IrEvaluator;

impl<'a> IrEvaluator<'a> {
    pub(crate) fn fn_array_length(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Array(arr)) => Ok(Value::Int64(arr.len() as i64)),
            _ => Err(Error::InvalidQuery(
                "ARRAY_LENGTH requires array argument".into(),
            )),
        }
    }

    pub(crate) fn fn_array_to_string(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ARRAY_TO_STRING requires at least 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) => Ok(Value::Null),
            (Value::Array(arr), Value::String(sep)) => {
                let strs: Vec<String> = arr
                    .iter()
                    .filter(|v| !v.is_null())
                    .map(|v| format!("{}", v))
                    .collect();
                Ok(Value::String(strs.join(sep)))
            }
            _ => Err(Error::InvalidQuery(
                "ARRAY_TO_STRING requires array and string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_generate_array(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "GENERATE_ARRAY requires at least 2 arguments".into(),
            ));
        }
        let start = match &args[0] {
            Value::Int64(n) => *n,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_ARRAY requires integer arguments".into(),
                ));
            }
        };
        let end = match &args[1] {
            Value::Int64(n) => *n,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_ARRAY requires integer arguments".into(),
                ));
            }
        };
        let step = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
        if step == 0 {
            return Err(Error::InvalidQuery(
                "GENERATE_ARRAY step cannot be zero".into(),
            ));
        }
        let mut result = Vec::new();
        let mut i = start;
        if step > 0 {
            while i <= end {
                result.push(Value::Int64(i));
                i += step;
            }
        } else {
            while i >= end {
                result.push(Value::Int64(i));
                i += step;
            }
        }
        Ok(Value::Array(result))
    }

    pub(crate) fn fn_array_concat(&self, args: &[Value]) -> Result<Value> {
        let mut result = Vec::new();
        for arg in args {
            match arg {
                Value::Null => continue,
                Value::Array(arr) => result.extend(arr.clone()),
                _ => {
                    return Err(Error::InvalidQuery(
                        "ARRAY_CONCAT requires array arguments".into(),
                    ));
                }
            }
        }
        Ok(Value::Array(result))
    }

    pub(crate) fn fn_array_reverse(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Array(arr)) => {
                let mut reversed = arr.clone();
                reversed.reverse();
                Ok(Value::Array(reversed))
            }
            _ => Err(Error::InvalidQuery(
                "ARRAY_REVERSE requires array argument".into(),
            )),
        }
    }

    pub(crate) fn fn_offset(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "OFFSET requires array and index arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Array(arr), Value::Int64(idx)) => {
                if *idx < 0 || *idx as usize >= arr.len() {
                    Err(Error::InvalidQuery(format!(
                        "Array index {} out of bounds for array of length {}",
                        idx,
                        arr.len()
                    )))
                } else {
                    Ok(arr[*idx as usize].clone())
                }
            }
            _ => Err(Error::InvalidQuery(
                "OFFSET requires array and integer arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_ordinal(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ORDINAL requires array and index arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Array(arr), Value::Int64(idx)) => {
                let zero_based = *idx - 1;
                if zero_based < 0 || zero_based as usize >= arr.len() {
                    Err(Error::InvalidQuery(format!(
                        "Array ordinal {} out of bounds for array of length {}",
                        idx,
                        arr.len()
                    )))
                } else {
                    Ok(arr[zero_based as usize].clone())
                }
            }
            _ => Err(Error::InvalidQuery(
                "ORDINAL requires array and integer arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_array_slice(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "ARRAY_SLICE requires array, start, and end arguments".into(),
            ));
        }
        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
            (Value::Array(arr), Value::Int64(start), Value::Int64(end)) => {
                let start_idx = if *start < 0 {
                    (arr.len() as i64 + start).max(0) as usize
                } else if *start == 0 {
                    0
                } else {
                    ((*start - 1) as usize).min(arr.len())
                };
                let end_idx = if *end < 0 {
                    (arr.len() as i64 + end).max(0) as usize
                } else {
                    (*end as usize).min(arr.len())
                };
                if start_idx >= end_idx {
                    Ok(Value::Array(vec![]))
                } else {
                    Ok(Value::Array(arr[start_idx..end_idx].to_vec()))
                }
            }
            _ => Err(Error::InvalidQuery(
                "ARRAY_SLICE expects array and integer arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_array_enumerate(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) | None => Ok(Value::Null),
            Some(Value::Array(arr)) => {
                let indices: Vec<Value> = (1..=arr.len() as i64).map(Value::Int64).collect();
                Ok(Value::Array(indices))
            }
            _ => Err(Error::InvalidQuery(
                "arrayEnumerate expects an array argument".into(),
            )),
        }
    }
}
