use ordered_float::OrderedFloat;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::super::helpers::extract_json_path;
use super::super::{IrEvaluator, value_to_json};

impl<'a> IrEvaluator<'a> {
    pub(crate) fn fn_to_json(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(v) => {
                let json = value_to_json(v)?;
                Ok(Value::Json(json))
            }
            None => Err(Error::InvalidQuery("TO_JSON requires an argument".into())),
        }
    }

    pub(crate) fn fn_to_json_string(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(v) => {
                let json = value_to_json(v)?;
                Ok(Value::String(json.to_string()))
            }
            None => Err(Error::InvalidQuery(
                "TO_JSON_STRING requires an argument".into(),
            )),
        }
    }

    pub(crate) fn fn_parse_json(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let json: serde_json::Value = serde_json::from_str(s)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?;
                Ok(Value::Json(json))
            }
            _ => Err(Error::InvalidQuery(
                "PARSE_JSON requires string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_json_value(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "JSON_VALUE requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Json(json), Value::String(path)) => {
                let value = extract_json_path(json, path)?;
                match value {
                    Some(serde_json::Value::String(s)) => Ok(Value::String(s)),
                    Some(serde_json::Value::Number(n)) => {
                        if let Some(i) = n.as_i64() {
                            Ok(Value::String(i.to_string()))
                        } else if let Some(f) = n.as_f64() {
                            Ok(Value::String(f.to_string()))
                        } else {
                            Ok(Value::String(n.to_string()))
                        }
                    }
                    Some(serde_json::Value::Bool(b)) => Ok(Value::String(b.to_string())),
                    Some(serde_json::Value::Null) => Ok(Value::Null),
                    Some(_) => Ok(Value::Null),
                    None => Ok(Value::Null),
                }
            }
            (Value::String(s), Value::String(path)) => {
                let json: serde_json::Value = serde_json::from_str(s)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?;
                let value = extract_json_path(&json, path)?;
                match value {
                    Some(serde_json::Value::String(s)) => Ok(Value::String(s)),
                    Some(serde_json::Value::Number(n)) => Ok(Value::String(n.to_string())),
                    Some(serde_json::Value::Bool(b)) => Ok(Value::String(b.to_string())),
                    Some(serde_json::Value::Null) => Ok(Value::Null),
                    Some(_) => Ok(Value::Null),
                    None => Ok(Value::Null),
                }
            }
            _ => Err(Error::InvalidQuery(
                "JSON_VALUE requires JSON and path arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_json_query(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "JSON_QUERY requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Json(json), Value::String(path)) => {
                let value = extract_json_path(json, path)?;
                match value {
                    Some(v) => Ok(Value::Json(v)),
                    None => Ok(Value::Null),
                }
            }
            (Value::String(s), Value::String(path)) => {
                let json: serde_json::Value = serde_json::from_str(s)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?;
                let value = extract_json_path(&json, path)?;
                match value {
                    Some(v) => Ok(Value::Json(v)),
                    None => Ok(Value::Null),
                }
            }
            _ => Err(Error::InvalidQuery(
                "JSON_QUERY requires JSON and path arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_json_extract(&self, args: &[Value]) -> Result<Value> {
        self.fn_json_value(args)
    }

    pub(crate) fn fn_json_extract_array(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        let json_val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Json(j) => j.clone(),
            Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(j) => j,
                Err(_) => return Ok(Value::Null),
            },
            _ => return Ok(Value::Null),
        };
        let json_to_extract = if args.len() > 1 {
            if let Value::String(path) = &args[1] {
                let path = path.trim_start_matches('$');
                let path = path.trim_start_matches('.');
                if path.is_empty() {
                    json_val
                } else {
                    let mut current = &json_val;
                    for part in path.split('.') {
                        let part = part.trim_start_matches('[').trim_end_matches(']');
                        if let Ok(idx) = part.parse::<usize>() {
                            if let Some(arr) = current.as_array() {
                                if idx < arr.len() {
                                    current = &arr[idx];
                                } else {
                                    return Ok(Value::Null);
                                }
                            } else {
                                return Ok(Value::Null);
                            }
                        } else if let Some(obj) = current.as_object() {
                            if let Some(val) = obj.get(part) {
                                current = val;
                            } else {
                                return Ok(Value::Null);
                            }
                        } else {
                            return Ok(Value::Null);
                        }
                    }
                    current.clone()
                }
            } else {
                json_val
            }
        } else {
            json_val
        };
        if let Some(arr) = json_to_extract.as_array() {
            let result: Vec<Value> = arr.iter().map(|v| Value::Json(v.clone())).collect();
            Ok(Value::Array(result))
        } else {
            Ok(Value::Array(vec![]))
        }
    }

    pub(crate) fn fn_json_type(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("JSON_TYPE requires 1 argument".into()));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(json) => {
                let type_name = match json {
                    serde_json::Value::Null => "null",
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::String(_) => "string",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::Object(_) => "object",
                };
                Ok(Value::String(type_name.to_string()))
            }
            _ => Err(Error::InvalidQuery(
                "JSON_TYPE requires a JSON argument".into(),
            )),
        }
    }

    pub(crate) fn fn_json_array(&self, args: &[Value]) -> Result<Value> {
        let json_values: Vec<serde_json::Value> = args
            .iter()
            .map(|v| match v {
                Value::Null => serde_json::Value::Null,
                Value::Bool(b) => serde_json::Value::Bool(*b),
                Value::Int64(n) => serde_json::Value::Number((*n).into()),
                Value::Float64(f) => serde_json::Number::from_f64(f.0)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
                Value::String(s) => serde_json::Value::String(s.clone()),
                _ => serde_json::Value::Null,
            })
            .collect();
        Ok(Value::Json(serde_json::Value::Array(json_values)))
    }

    pub(crate) fn fn_json_object(&self, args: &[Value]) -> Result<Value> {
        let mut obj = serde_json::Map::new();
        for chunk in args.chunks(2) {
            if chunk.len() == 2 {
                if let Value::String(key) = &chunk[0] {
                    let val = match &chunk[1] {
                        Value::Null => serde_json::Value::Null,
                        Value::Bool(b) => serde_json::Value::Bool(*b),
                        Value::Int64(n) => serde_json::Value::Number((*n).into()),
                        Value::Float64(f) => serde_json::Number::from_f64(f.0)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null),
                        Value::String(s) => serde_json::Value::String(s.clone()),
                        _ => serde_json::Value::Null,
                    };
                    obj.insert(key.clone(), val);
                }
            }
        }
        Ok(Value::Json(serde_json::Value::Object(obj)))
    }

    pub(crate) fn fn_json_set(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Ok(Value::Null);
        }
        let json_val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Json(j) => j.clone(),
            Value::String(s) => serde_json::from_str(s).unwrap_or(serde_json::Value::Null),
            _ => return Ok(Value::Null),
        };
        let path = match &args[1] {
            Value::String(p) => p.clone(),
            _ => return Ok(Value::Json(json_val)),
        };
        let new_value = self.value_to_json_local(&args[2]);

        let key = if path.starts_with("$.") {
            path[2..].to_string()
        } else if path.starts_with("$[") || path.starts_with('$') {
            path[1..].to_string()
        } else {
            path.clone()
        };

        let mut result = json_val;
        if let serde_json::Value::Object(ref mut obj) = result {
            obj.insert(key, new_value);
        }
        Ok(Value::Json(result))
    }

    fn value_to_json_local(&self, val: &Value) -> serde_json::Value {
        match val {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int64(n) => serde_json::json!(*n),
            Value::Float64(f) => serde_json::json!(f.0),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Json(j) => j.clone(),
            Value::Numeric(n) => serde_json::json!(n.to_string()),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| self.value_to_json_local(v)).collect())
            }
            _ => serde_json::Value::Null,
        }
    }

    pub(crate) fn fn_json_remove(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Ok(Value::Null);
        }
        let json_val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Json(j) => j.clone(),
            Value::String(s) => serde_json::from_str(s).unwrap_or(serde_json::Value::Null),
            _ => return Ok(Value::Null),
        };
        let path = match &args[1] {
            Value::String(p) => p.clone(),
            _ => return Ok(Value::Json(json_val)),
        };

        let key = if path.starts_with("$.") {
            path[2..].to_string()
        } else if path.starts_with("$[") || path.starts_with('$') {
            path[1..].to_string()
        } else {
            path.clone()
        };

        let mut result = json_val;
        if let serde_json::Value::Object(ref mut obj) = result {
            obj.remove(&key);
        }
        Ok(Value::Json(result))
    }

    pub(crate) fn fn_json_strip_nulls(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(j) => {
                fn strip_nulls(v: &serde_json::Value) -> serde_json::Value {
                    match v {
                        serde_json::Value::Object(obj) => {
                            let filtered: serde_json::Map<String, serde_json::Value> = obj
                                .iter()
                                .filter(|(_, val)| !val.is_null())
                                .map(|(k, val)| (k.clone(), strip_nulls(val)))
                                .collect();
                            serde_json::Value::Object(filtered)
                        }
                        serde_json::Value::Array(arr) => {
                            serde_json::Value::Array(arr.iter().map(strip_nulls).collect())
                        }
                        other => other.clone(),
                    }
                }
                Ok(Value::Json(strip_nulls(j)))
            }
            Value::String(s) => {
                let parsed: serde_json::Value =
                    serde_json::from_str(s).unwrap_or(serde_json::Value::Null);
                fn strip_nulls(v: &serde_json::Value) -> serde_json::Value {
                    match v {
                        serde_json::Value::Object(obj) => {
                            let filtered: serde_json::Map<String, serde_json::Value> = obj
                                .iter()
                                .filter(|(_, val)| !val.is_null())
                                .map(|(k, val)| (k.clone(), strip_nulls(val)))
                                .collect();
                            serde_json::Value::Object(filtered)
                        }
                        serde_json::Value::Array(arr) => {
                            serde_json::Value::Array(arr.iter().map(strip_nulls).collect())
                        }
                        other => other.clone(),
                    }
                }
                Ok(Value::Json(strip_nulls(&parsed)))
            }
            _ => Ok(Value::Null),
        }
    }

    pub(crate) fn fn_json_query_array(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        let json_val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Json(j) => j.clone(),
            Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(j) => j,
                Err(_) => return Ok(Value::Array(vec![])),
            },
            _ => return Ok(Value::Null),
        };
        let json_to_extract = if args.len() > 1 {
            if let Value::String(path) = &args[1] {
                match extract_json_path(&json_val, path)? {
                    Some(v) => v,
                    None => return Ok(Value::Array(vec![])),
                }
            } else {
                json_val
            }
        } else {
            json_val
        };
        if let Some(arr) = json_to_extract.as_array() {
            let values: Vec<Value> = arr.iter().map(|v| Value::Json(v.clone())).collect();
            Ok(Value::Array(values))
        } else {
            Ok(Value::Array(vec![]))
        }
    }

    pub(crate) fn fn_json_value_array(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        let json_val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Json(j) => j.clone(),
            Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(j) => j,
                Err(_) => return Ok(Value::Array(vec![])),
            },
            _ => return Ok(Value::Null),
        };
        let json_to_extract = if args.len() > 1 {
            if let Value::String(path) = &args[1] {
                match extract_json_path(&json_val, path)? {
                    Some(v) => v,
                    None => return Ok(Value::Array(vec![])),
                }
            } else {
                json_val
            }
        } else {
            json_val
        };
        if let Some(arr) = json_to_extract.as_array() {
            let values: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => Value::String(s.clone()),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Value::Int64(i)
                        } else if let Some(f) = n.as_f64() {
                            Value::Float64(OrderedFloat(f))
                        } else {
                            Value::Null
                        }
                    }
                    serde_json::Value::Bool(b) => Value::Bool(*b),
                    _ => Value::Null,
                })
                .collect();
            Ok(Value::Array(values))
        } else {
            Ok(Value::Array(vec![]))
        }
    }

    pub(crate) fn fn_json_extract_string_array(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        let json_val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Json(j) => j.clone(),
            Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(j) => j,
                Err(_) => return Ok(Value::Array(vec![])),
            },
            _ => return Ok(Value::Null),
        };
        let json_to_extract = if args.len() > 1 {
            if let Value::String(path) = &args[1] {
                match extract_json_path(&json_val, path)? {
                    Some(v) => v,
                    None => return Ok(Value::Array(vec![])),
                }
            } else {
                json_val
            }
        } else {
            json_val
        };
        if let Some(arr) = json_to_extract.as_array() {
            let values: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => Value::String(s.clone()),
                    serde_json::Value::Number(n) => Value::String(n.to_string()),
                    serde_json::Value::Bool(b) => Value::String(b.to_string()),
                    serde_json::Value::Null => Value::Null,
                    _ => Value::String(v.to_string()),
                })
                .collect();
            Ok(Value::Array(values))
        } else {
            Ok(Value::Array(vec![]))
        }
    }

    pub(crate) fn fn_json_keys(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(j) => {
                if let Some(obj) = j.as_object() {
                    let keys: Vec<Value> = obj.keys().map(|k| Value::String(k.clone())).collect();
                    Ok(Value::Array(keys))
                } else {
                    Ok(Value::Array(vec![]))
                }
            }
            Value::String(s) => {
                if let Ok(j) = serde_json::from_str::<serde_json::Value>(s) {
                    if let Some(obj) = j.as_object() {
                        let keys: Vec<Value> =
                            obj.keys().map(|k| Value::String(k.clone())).collect();
                        return Ok(Value::Array(keys));
                    }
                }
                Ok(Value::Array(vec![]))
            }
            _ => Ok(Value::Null),
        }
    }
}
