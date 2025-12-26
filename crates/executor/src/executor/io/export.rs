use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use chrono::Datelike;
use parquet::arrow::arrow_writer::ArrowWriter;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_storage::{Schema, Table};

pub fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int64(n) => serde_json::Value::Number((*n).into()),
        Value::Float64(f) => serde_json::json!(f.0),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Date(d) => serde_json::Value::String(d.to_string()),
        Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
        Value::Timestamp(ts) => serde_json::Value::String(ts.to_rfc3339()),
        Value::Time(t) => serde_json::Value::String(t.to_string()),
        Value::Numeric(n) => serde_json::Value::String(n.to_string()),
        Value::BigNumeric(n) => serde_json::Value::String(n.to_string()),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Struct(fields) => {
            let obj: serde_json::Map<String, serde_json::Value> = fields
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        Value::Json(j) => j.clone(),
        Value::Bytes(_)
        | Value::Geography(_)
        | Value::Interval(_)
        | Value::Range(_)
        | Value::Default => serde_json::Value::String(format!("{:?}", val)),
    }
}

pub fn value_to_csv_string(val: &Value) -> String {
    match val {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.0.to_string(),
        Value::String(s) => {
            if s.contains(',') || s.contains('"') || s.contains('\n') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s.clone()
            }
        }
        Value::Date(d) => d.to_string(),
        Value::DateTime(dt) => dt.to_string(),
        Value::Timestamp(ts) => ts.to_rfc3339(),
        Value::Time(t) => t.to_string(),
        Value::Numeric(n) => n.to_string(),
        Value::BigNumeric(n) => n.to_string(),
        Value::Bytes(_)
        | Value::Array(_)
        | Value::Struct(_)
        | Value::Json(_)
        | Value::Geography(_)
        | Value::Interval(_)
        | Value::Range(_)
        | Value::Default => format!("{:?}", val),
    }
}

pub fn export_to_json(data: &Table, path: &str) -> Result<()> {
    let schema = data.schema();
    let mut file = File::create(path)
        .map_err(|e| Error::internal(format!("Failed to create file '{}': {}", path, e)))?;

    for record in data.rows()? {
        let mut obj = serde_json::Map::new();
        for (i, field) in schema.fields().iter().enumerate() {
            let val = &record.values()[i];
            obj.insert(field.name.clone(), value_to_json(val));
        }
        let line = serde_json::to_string(&obj)
            .map_err(|e| Error::internal(format!("JSON serialization error: {}", e)))?;
        writeln!(file, "{}", line)
            .map_err(|e| Error::internal(format!("Failed to write JSON: {}", e)))?;
    }
    Ok(())
}

pub fn export_to_csv(
    data: &Table,
    path: &str,
    delimiter: char,
    include_header: bool,
) -> Result<()> {
    let schema = data.schema();
    let mut file = File::create(path)
        .map_err(|e| Error::internal(format!("Failed to create file '{}': {}", path, e)))?;

    if include_header {
        let header: Vec<&str> = schema.fields().iter().map(|f| f.name.as_str()).collect();
        writeln!(file, "{}", header.join(&delimiter.to_string()))
            .map_err(|e| Error::internal(format!("Failed to write CSV header: {}", e)))?;
    }

    for record in data.rows()? {
        let values: Vec<String> = record.values().iter().map(value_to_csv_string).collect();
        writeln!(file, "{}", values.join(&delimiter.to_string()))
            .map_err(|e| Error::internal(format!("Failed to write CSV row: {}", e)))?;
    }
    Ok(())
}

pub fn export_to_parquet(data: &Table, path: &str) -> Result<()> {
    let schema = data.schema();
    let num_rows = data.num_rows();

    let arrow_fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .map(|f| {
            let arrow_type = data_type_to_arrow(&f.data_type);
            ArrowField::new(&f.name, arrow_type, f.is_nullable())
        })
        .collect();
    let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

    let arrays = build_arrow_arrays(data, schema, num_rows)?;

    let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
        .map_err(|e| Error::internal(format!("Failed to create RecordBatch: {}", e)))?;

    let file = File::create(path)
        .map_err(|e| Error::internal(format!("Failed to create file '{}': {}", path, e)))?;
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None)
        .map_err(|e| Error::internal(format!("Failed to create Parquet writer: {}", e)))?;
    writer
        .write(&batch)
        .map_err(|e| Error::internal(format!("Failed to write Parquet data: {}", e)))?;
    writer
        .close()
        .map_err(|e| Error::internal(format!("Failed to close Parquet writer: {}", e)))?;

    Ok(())
}

pub fn data_type_to_arrow(data_type: &DataType) -> ArrowDataType {
    match data_type {
        DataType::Bool => ArrowDataType::Boolean,
        DataType::Int64 => ArrowDataType::Int64,
        DataType::Float64 => ArrowDataType::Float64,
        DataType::String => ArrowDataType::Utf8,
        DataType::Date => ArrowDataType::Date32,
        DataType::DateTime => {
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        DataType::Timestamp => ArrowDataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into()),
        ),
        DataType::Time => ArrowDataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
        DataType::Bytes => ArrowDataType::Binary,
        DataType::Numeric(_)
        | DataType::Unknown
        | DataType::BigNumeric
        | DataType::Geography
        | DataType::Json
        | DataType::Struct(_)
        | DataType::Array(_)
        | DataType::Interval
        | DataType::Range(_) => ArrowDataType::Utf8,
    }
}

pub fn build_arrow_arrays(
    data: &Table,
    schema: &Schema,
    _num_rows: usize,
) -> Result<Vec<ArrayRef>> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array: ArrayRef = match &field.data_type {
            DataType::Bool => {
                let mut builder = BooleanBuilder::new();
                for record in data.rows()? {
                    let val = &record.values()[col_idx];
                    match val {
                        Value::Bool(b) => builder.append_value(*b),
                        Value::Null
                        | Value::Int64(_)
                        | Value::Float64(_)
                        | Value::String(_)
                        | Value::Date(_)
                        | Value::Time(_)
                        | Value::DateTime(_)
                        | Value::Timestamp(_)
                        | Value::Numeric(_)
                        | Value::BigNumeric(_)
                        | Value::Bytes(_)
                        | Value::Json(_)
                        | Value::Array(_)
                        | Value::Struct(_)
                        | Value::Geography(_)
                        | Value::Interval(_)
                        | Value::Range(_)
                        | Value::Default => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::new();
                for record in data.rows()? {
                    let val = &record.values()[col_idx];
                    match val {
                        Value::Int64(n) => builder.append_value(*n),
                        Value::Float64(f) => builder.append_value(f.0 as i64),
                        Value::Null
                        | Value::Bool(_)
                        | Value::Numeric(_)
                        | Value::BigNumeric(_)
                        | Value::String(_)
                        | Value::Bytes(_)
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
                        | Value::Default => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::new();
                for record in data.rows()? {
                    let val = &record.values()[col_idx];
                    match val {
                        Value::Float64(f) => builder.append_value(f.0),
                        Value::Int64(n) => builder.append_value(*n as f64),
                        Value::Null
                        | Value::Bool(_)
                        | Value::Numeric(_)
                        | Value::BigNumeric(_)
                        | Value::String(_)
                        | Value::Bytes(_)
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
                        | Value::Default => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Date => {
                let mut builder = Date32Builder::new();
                for record in data.rows()? {
                    let val = &record.values()[col_idx];
                    match val {
                        Value::Date(d) => {
                            let days = d.num_days_from_ce() - 719163;
                            builder.append_value(days);
                        }
                        Value::Null
                        | Value::Bool(_)
                        | Value::Int64(_)
                        | Value::Float64(_)
                        | Value::String(_)
                        | Value::Time(_)
                        | Value::DateTime(_)
                        | Value::Timestamp(_)
                        | Value::Numeric(_)
                        | Value::BigNumeric(_)
                        | Value::Bytes(_)
                        | Value::Json(_)
                        | Value::Array(_)
                        | Value::Struct(_)
                        | Value::Geography(_)
                        | Value::Interval(_)
                        | Value::Range(_)
                        | Value::Default => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::DateTime | DataType::Timestamp => {
                let mut builder = TimestampMicrosecondBuilder::new();
                for record in data.rows()? {
                    let val = &record.values()[col_idx];
                    match val {
                        Value::Null => builder.append_null(),
                        Value::DateTime(dt) => {
                            let micros = dt.and_utc().timestamp_micros();
                            builder.append_value(micros);
                        }
                        Value::Timestamp(ts) => {
                            builder.append_value(ts.timestamp_micros());
                        }
                        Value::Bool(_)
                        | Value::Int64(_)
                        | Value::Float64(_)
                        | Value::String(_)
                        | Value::Date(_)
                        | Value::Time(_)
                        | Value::Numeric(_)
                        | Value::BigNumeric(_)
                        | Value::Bytes(_)
                        | Value::Json(_)
                        | Value::Array(_)
                        | Value::Struct(_)
                        | Value::Geography(_)
                        | Value::Interval(_)
                        | Value::Range(_)
                        | Value::Default => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Unknown
            | DataType::Numeric(_)
            | DataType::BigNumeric
            | DataType::String
            | DataType::Bytes
            | DataType::Time
            | DataType::Geography
            | DataType::Json
            | DataType::Struct(_)
            | DataType::Array(_)
            | DataType::Interval
            | DataType::Range(_) => {
                let mut builder = StringBuilder::new();
                for record in data.rows()? {
                    let val = &record.values()[col_idx];
                    match val {
                        Value::Null => builder.append_null(),
                        Value::Bool(b) => builder.append_value(b.to_string()),
                        Value::Int64(n) => builder.append_value(n.to_string()),
                        Value::Float64(f) => builder.append_value(f.0.to_string()),
                        Value::String(s) => builder.append_value(s),
                        Value::Date(d) => builder.append_value(d.to_string()),
                        Value::DateTime(dt) => builder.append_value(dt.to_string()),
                        Value::Timestamp(ts) => builder.append_value(ts.to_rfc3339()),
                        Value::Time(t) => builder.append_value(t.to_string()),
                        Value::Numeric(n) => builder.append_value(n.to_string()),
                        Value::BigNumeric(n) => builder.append_value(n.to_string()),
                        Value::Bytes(b) => builder.append_value(hex::encode(b)),
                        Value::Json(j) => builder.append_value(j.to_string()),
                        Value::Array(a) => builder.append_value(format!("{:?}", a)),
                        Value::Struct(s) => builder.append_value(format!("{:?}", s)),
                        Value::Geography(g) => builder.append_value(g),
                        Value::Interval(i) => builder.append_value(format!("{:?}", i)),
                        Value::Range(r) => builder.append_value(format!("{:?}", r)),
                        Value::Default => builder.append_value("DEFAULT"),
                    }
                }
                Arc::new(builder.finish())
            }
        };
        arrays.push(array);
    }

    Ok(arrays)
}
