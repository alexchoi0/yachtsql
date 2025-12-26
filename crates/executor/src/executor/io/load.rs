use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType as ArrowDataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::LoadOptions;
use yachtsql_storage::Schema;

pub fn load_json(path: &str, schema: &Schema) -> Result<Vec<Vec<Value>>> {
    let file = File::open(path)
        .map_err(|e| Error::internal(format!("Failed to open file '{}': {}", path, e)))?;
    let reader = BufReader::new(file);

    let target_columns: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
    let target_types: Vec<DataType> = schema
        .fields()
        .iter()
        .map(|f| f.data_type.clone())
        .collect();

    let mut rows = Vec::new();

    for line in reader.lines() {
        let line =
            line.map_err(|e| Error::internal(format!("Failed to read JSON line: {}", e)))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let obj: HashMap<String, serde_json::Value> = serde_json::from_str(trimmed)
            .map_err(|e| Error::internal(format!("Failed to parse JSON: {}", e)))?;

        let mut row_values = Vec::with_capacity(target_columns.len());

        for (i, col_name) in target_columns.iter().enumerate() {
            let json_val = obj
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(col_name))
                .map(|(_, v)| v);

            let value = match json_val {
                Some(v) => json_to_value(v, &target_types[i])?,
                None => Value::Null,
            };
            row_values.push(value);
        }

        rows.push(row_values);
    }

    Ok(rows)
}

pub fn json_to_value(json_val: &serde_json::Value, target_type: &DataType) -> Result<Value> {
    match json_val {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                match target_type {
                    DataType::Float64 => {
                        Ok(Value::Float64(ordered_float::OrderedFloat(i as f64)))
                    }
                    DataType::Bool
                    | DataType::Int64
                    | DataType::String
                    | DataType::Bytes
                    | DataType::Numeric(_)
                    | DataType::BigNumeric
                    | DataType::Date
                    | DataType::Time
                    | DataType::DateTime
                    | DataType::Timestamp
                    | DataType::Interval
                    | DataType::Geography
                    | DataType::Json
                    | DataType::Array(_)
                    | DataType::Struct(_)
                    | DataType::Range(_)
                    | DataType::Unknown => Ok(Value::Int64(i)),
                }
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float64(ordered_float::OrderedFloat(f)))
            } else {
                Ok(Value::Null)
            }
        }
        serde_json::Value::String(s) => match target_type {
            DataType::Date => {
                let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|e| Error::internal(format!("Invalid date: {}", e)))?;
                Ok(Value::Date(date))
            }
            DataType::DateTime => {
                let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                    .map_err(|e| Error::internal(format!("Invalid datetime: {}", e)))?;
                Ok(Value::DateTime(dt))
            }
            DataType::Timestamp => {
                let ts = chrono::DateTime::parse_from_rfc3339(s)
                    .map(|d| d.with_timezone(&chrono::Utc))
                    .map_err(|e| Error::internal(format!("Invalid timestamp: {}", e)))?;
                Ok(Value::Timestamp(ts))
            }
            DataType::Bool
            | DataType::Int64
            | DataType::Float64
            | DataType::String
            | DataType::Bytes
            | DataType::Numeric(_)
            | DataType::BigNumeric
            | DataType::Time
            | DataType::Interval
            | DataType::Geography
            | DataType::Json
            | DataType::Array(_)
            | DataType::Struct(_)
            | DataType::Range(_)
            | DataType::Unknown => Ok(Value::String(s.clone())),
        },
        serde_json::Value::Array(arr) => {
            let values: Result<Vec<Value>> = arr
                .iter()
                .map(|v| json_to_value(v, &DataType::Unknown))
                .collect();
            Ok(Value::Array(values?))
        }
        serde_json::Value::Object(obj) => {
            let fields: Result<Vec<(String, Value)>> = obj
                .iter()
                .map(|(k, v)| {
                    let val = json_to_value(v, &DataType::Unknown)?;
                    Ok((k.clone(), val))
                })
                .collect();
            Ok(Value::Struct(fields?))
        }
    }
}

pub fn load_csv(path: &str, schema: &Schema, options: &LoadOptions) -> Result<Vec<Vec<Value>>> {
    let file = File::open(path)
        .map_err(|e| Error::internal(format!("Failed to open file '{}': {}", path, e)))?;
    let reader = BufReader::new(file);

    let delimiter = options
        .field_delimiter
        .as_ref()
        .and_then(|d| {
            if d == "\\t" || d == "\t" {
                Some('\t')
            } else {
                d.chars().next()
            }
        })
        .unwrap_or(',');

    let null_marker = options.null_marker.as_deref();
    let skip_rows = options.skip_leading_rows.unwrap_or(0) as usize;

    let target_types: Vec<DataType> = schema
        .fields()
        .iter()
        .map(|f| f.data_type.clone())
        .collect();

    let mut rows = Vec::new();

    for (idx, line) in reader.lines().enumerate() {
        if idx < skip_rows {
            continue;
        }

        let line =
            line.map_err(|e| Error::internal(format!("Failed to read CSV line: {}", e)))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let fields = parse_csv_line(trimmed, delimiter);
        let mut row_values = Vec::with_capacity(target_types.len());

        for (i, field_str) in fields.iter().enumerate() {
            if i >= target_types.len() {
                break;
            }
            let value = if null_marker.is_some_and(|nm| field_str == nm) {
                Value::Null
            } else {
                csv_to_value(field_str, &target_types[i])?
            };
            row_values.push(value);
        }

        while row_values.len() < target_types.len() {
            row_values.push(Value::Null);
        }

        rows.push(row_values);
    }

    Ok(rows)
}

pub fn parse_csv_line(line: &str, delimiter: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '"' {
            if in_quotes {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                in_quotes = true;
            }
        } else if c == delimiter && !in_quotes {
            fields.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(c);
        }
    }
    fields.push(current.trim().to_string());
    fields
}

pub fn csv_to_value(s: &str, target_type: &DataType) -> Result<Value> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Ok(Value::Null);
    }

    match target_type {
        DataType::Int64 => trimmed
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|e| Error::internal(format!("Invalid int64: {}", e))),
        DataType::Float64 => trimmed
            .parse::<f64>()
            .map(|f| Value::Float64(ordered_float::OrderedFloat(f)))
            .map_err(|e| Error::internal(format!("Invalid float64: {}", e))),
        DataType::Bool => trimmed
            .to_lowercase()
            .parse::<bool>()
            .map(Value::Bool)
            .map_err(|e| Error::internal(format!("Invalid bool: {}", e))),
        DataType::Date => chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
            .map(Value::Date)
            .map_err(|e| Error::internal(format!("Invalid date: {}", e))),
        DataType::DateTime => {
            chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S"))
                .map(Value::DateTime)
                .map_err(|e| Error::internal(format!("Invalid datetime: {}", e)))
        }
        DataType::String
        | DataType::Bytes
        | DataType::Time
        | DataType::Timestamp
        | DataType::Numeric(_)
        | DataType::BigNumeric
        | DataType::Interval
        | DataType::Geography
        | DataType::Json
        | DataType::Array(_)
        | DataType::Struct(_)
        | DataType::Range(_)
        | DataType::Unknown => Ok(Value::String(trimmed.to_string())),
    }
}

pub fn load_parquet(path: &str, schema: &Schema) -> Result<Vec<Vec<Value>>> {
    let file = File::open(path)
        .map_err(|e| Error::internal(format!("Failed to open file '{}': {}", path, e)))?;

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::internal(format!("Failed to read Parquet: {}", e)))?
        .build()
        .map_err(|e| Error::internal(format!("Failed to build Parquet reader: {}", e)))?;

    let mut rows = Vec::new();
    let target_columns: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
    let target_types: Vec<DataType> = schema
        .fields()
        .iter()
        .map(|f| f.data_type.clone())
        .collect();

    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| Error::internal(format!("Failed to read batch: {}", e)))?;

        let parquet_schema = batch.schema();
        let parquet_columns: Vec<String> = parquet_schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();

        let column_mapping: Vec<Option<usize>> = target_columns
            .iter()
            .map(|target_col| {
                parquet_columns
                    .iter()
                    .position(|pc| pc.eq_ignore_ascii_case(target_col))
            })
            .collect();

        for row_idx in 0..batch.num_rows() {
            let mut row_values = Vec::with_capacity(target_columns.len());

            for (col_idx, parquet_col_idx) in column_mapping.iter().enumerate() {
                let value = match parquet_col_idx {
                    Some(pci) => {
                        let array = batch.column(*pci);
                        arrow_array_to_value(array, row_idx, &target_types[col_idx])?
                    }
                    None => Value::null(),
                };
                row_values.push(value);
            }
            rows.push(row_values);
        }
    }

    Ok(rows)
}

pub fn arrow_array_to_value(
    array: &Arc<dyn Array>,
    row_idx: usize,
    target_type: &DataType,
) -> Result<Value> {
    if array.is_null(row_idx) {
        return Ok(Value::null());
    }

    let value = match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_boolean();
            Value::bool_val(arr.value(row_idx))
        }
        ArrowDataType::Int64 => {
            let arr = array.as_primitive::<arrow::datatypes::Int64Type>();
            Value::int64(arr.value(row_idx))
        }
        ArrowDataType::Int32 => {
            let arr = array.as_primitive::<arrow::datatypes::Int32Type>();
            Value::int64(arr.value(row_idx) as i64)
        }
        ArrowDataType::Float64 => {
            let arr = array.as_primitive::<arrow::datatypes::Float64Type>();
            Value::float64(arr.value(row_idx))
        }
        ArrowDataType::Float32 => {
            let arr = array.as_primitive::<arrow::datatypes::Float32Type>();
            Value::float64(arr.value(row_idx) as f64)
        }
        ArrowDataType::Utf8 => {
            let arr = array.as_string::<i32>();
            Value::string(arr.value(row_idx).to_string())
        }
        ArrowDataType::LargeUtf8 => {
            let arr = array.as_string::<i64>();
            Value::string(arr.value(row_idx).to_string())
        }
        ArrowDataType::Date32 => {
            let arr = array.as_primitive::<arrow::datatypes::Date32Type>();
            let days = arr.value(row_idx);
            let date =
                chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163).unwrap_or_default();
            Value::date(date)
        }
        ArrowDataType::Date64 => {
            let arr = array.as_primitive::<arrow::datatypes::Date64Type>();
            let millis = arr.value(row_idx);
            let date = chrono::DateTime::from_timestamp_millis(millis)
                .map(|dt| dt.naive_utc().date())
                .unwrap_or_default();
            Value::date(date)
        }
        ArrowDataType::Timestamp(_, _) => {
            let arr = array.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
            let micros = arr.value(row_idx);
            match target_type {
                DataType::DateTime => {
                    let dt = chrono::DateTime::from_timestamp_micros(micros)
                        .map(|dt| dt.naive_utc())
                        .unwrap_or_default();
                    Value::datetime(dt)
                }
                DataType::Bool
                | DataType::Int64
                | DataType::Float64
                | DataType::String
                | DataType::Bytes
                | DataType::Numeric(_)
                | DataType::BigNumeric
                | DataType::Date
                | DataType::Time
                | DataType::Timestamp
                | DataType::Interval
                | DataType::Geography
                | DataType::Json
                | DataType::Array(_)
                | DataType::Struct(_)
                | DataType::Range(_)
                | DataType::Unknown => {
                    let ts = chrono::DateTime::from_timestamp_micros(micros).unwrap_or_default();
                    Value::timestamp(ts)
                }
            }
        }
        ArrowDataType::Null
        | ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Float16
        | ArrowDataType::Time32(_)
        | ArrowDataType::Time64(_)
        | ArrowDataType::Duration(_)
        | ArrowDataType::Interval(_)
        | ArrowDataType::Binary
        | ArrowDataType::FixedSizeBinary(_)
        | ArrowDataType::LargeBinary
        | ArrowDataType::BinaryView
        | ArrowDataType::Utf8View
        | ArrowDataType::List(_)
        | ArrowDataType::ListView(_)
        | ArrowDataType::FixedSizeList(_, _)
        | ArrowDataType::LargeList(_)
        | ArrowDataType::LargeListView(_)
        | ArrowDataType::Struct(_)
        | ArrowDataType::Union(_, _)
        | ArrowDataType::Dictionary(_, _)
        | ArrowDataType::Decimal128(_, _)
        | ArrowDataType::Decimal256(_, _)
        | ArrowDataType::Map(_, _)
        | ArrowDataType::RunEndEncoded(_, _) => Value::string(format!("{:?}", array)),
    };

    Ok(value)
}
