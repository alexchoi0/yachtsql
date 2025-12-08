use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, TimeZone, Utc};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_ir::FunctionName;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_introspection_function(
        name: &FunctionName,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            FunctionName::CurrentDatabase => Ok(Value::string("default".to_string())),
            FunctionName::CurrentUser => Ok(Value::string("default".to_string())),
            FunctionName::Version => Ok(Value::string("1.0.0".to_string())),
            FunctionName::Uptime => {
                let uptime = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs() % 86400)
                    .unwrap_or(0);
                Ok(Value::int64(uptime as i64))
            }
            FunctionName::Timezone => Ok(Value::string("UTC".to_string())),
            FunctionName::ServerTimezone => Ok(Value::string("UTC".to_string())),
            FunctionName::BlockNumber => Ok(Value::int64(0)),
            FunctionName::RowNumberInBlock => Ok(Value::int64(row_idx as i64)),
            FunctionName::RowNumberInAllBlocks => Ok(Value::int64(row_idx as i64)),
            FunctionName::HostName => Ok(Value::string("localhost".to_string())),
            FunctionName::Fqdn => Ok(Value::string("localhost".to_string())),
            FunctionName::IsFinite => Self::eval_is_finite(args, batch, row_idx),
            FunctionName::IsInfinite => Self::eval_is_infinite(args, batch, row_idx),
            FunctionName::IsNan => Self::eval_is_nan(args, batch, row_idx),
            FunctionName::ToTypeName => Self::eval_to_type_name(args, batch, row_idx),
            FunctionName::DumpColumnStructure => {
                Self::eval_dump_column_structure(args, batch, row_idx)
            }
            FunctionName::DefaultValueOfArgumentType => {
                Self::eval_default_value_of_argument_type(args, batch, row_idx)
            }
            FunctionName::DefaultValueOfTypeName => {
                Self::eval_default_value_of_type_name(args, batch, row_idx)
            }
            FunctionName::BlockSize => Ok(Value::int64(batch.num_rows() as i64)),
            FunctionName::CurrentSchemas => {
                Ok(Value::array(vec![Value::string("default".to_string())]))
            }
            FunctionName::QueryId => Ok(Value::string(
                "00000000-0000-0000-0000-000000000000".to_string(),
            )),
            FunctionName::InitialQueryId => Ok(Value::string(
                "00000000-0000-0000-0000-000000000000".to_string(),
            )),
            FunctionName::ServerUuid => Ok(Value::string(
                "00000000-0000-0000-0000-000000000000".to_string(),
            )),
            FunctionName::GetSetting => Self::eval_get_setting(args, batch, row_idx),
            FunctionName::IsDecimalOverflow => Self::eval_is_decimal_overflow(args, batch, row_idx),
            FunctionName::CountDigits => Self::eval_count_digits(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown introspection function: {}",
                name.as_str()
            ))),
        }
    }

    fn eval_is_finite(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "isFinite requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = val.as_f64().unwrap_or(0.0);
        Ok(Value::bool_val(f.is_finite()))
    }

    fn eval_is_infinite(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "isInfinite requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = val.as_f64().unwrap_or(0.0);
        Ok(Value::bool_val(f.is_infinite()))
    }

    fn eval_is_nan(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("isNaN requires 1 argument".to_string()));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let f = val.as_f64().unwrap_or(0.0);
        Ok(Value::bool_val(f.is_nan()))
    }

    fn eval_to_type_name(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "toTypeName requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let type_name = Self::value_type_name(&val);
        Ok(Value::string(type_name.to_string()))
    }

    fn value_type_name(val: &Value) -> &'static str {
        if val.is_null() {
            "Nullable(Nothing)"
        } else if val.is_int64() {
            "Int64"
        } else if val.is_float64() {
            "Float64"
        } else if val.is_bool() {
            "UInt8"
        } else if val.is_string() {
            "String"
        } else if val.is_numeric() {
            "Decimal"
        } else if val.is_array() {
            "Array"
        } else if val.as_struct().is_some() {
            "Tuple"
        } else if val.is_map() {
            "Map"
        } else if val.as_uuid().is_some() {
            "UUID"
        } else if val.is_json() {
            "JSON"
        } else if val.as_date().is_some() {
            "Date"
        } else if val.as_time().is_some() || val.as_timestamp().is_some() {
            "DateTime"
        } else if val.as_bytes().is_some() {
            "String"
        } else {
            "Unknown"
        }
    }

    fn eval_dump_column_structure(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "dumpColumnStructure requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let type_name = Self::value_type_name(&val);
        Ok(Value::string(format!("{}: {}", type_name, val)))
    }

    fn eval_default_value_of_argument_type(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "defaultValueOfArgumentType requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        Ok(Self::default_value_for_type(&val))
    }

    fn default_value_for_type(val: &Value) -> Value {
        if val.is_null() {
            Value::null()
        } else if val.is_int64() {
            Value::int64(0)
        } else if val.is_float64() {
            Value::float64(0.0)
        } else if val.is_bool() {
            Value::bool_val(false)
        } else if val.is_string() {
            Value::string(String::new())
        } else if val.as_bytes().is_some() {
            Value::bytes(vec![])
        } else if val.as_date().is_some() {
            Value::date(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        } else if val.as_time().is_some() {
            Value::time(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
        } else if val.as_timestamp().is_some() {
            Value::timestamp(Utc.timestamp_opt(0, 0).unwrap())
        } else if val.is_numeric() {
            Value::numeric(rust_decimal::Decimal::ZERO)
        } else if val.is_array() {
            Value::array(vec![])
        } else if val.as_struct().is_some() {
            Value::struct_val(indexmap::IndexMap::new())
        } else if val.is_map() {
            Value::map(vec![])
        } else if val.as_uuid().is_some() {
            Value::uuid(uuid::Uuid::nil())
        } else if val.is_json() {
            Value::json(serde_json::Value::Null)
        } else {
            Value::null()
        }
    }

    fn eval_default_value_of_type_name(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "defaultValueOfTypeName requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let type_str = val.to_string().to_uppercase();

        match type_str.as_str() {
            "INT8" | "INT16" | "INT32" | "INT64" | "UINT8" | "UINT16" | "UINT32" | "UINT64"
            | "BIGINT" | "INTEGER" | "INT" => Ok(Value::int64(0)),
            "FLOAT32" | "FLOAT64" | "FLOAT" | "DOUBLE" => Ok(Value::float64(0.0)),
            "STRING" | "VARCHAR" | "TEXT" => Ok(Value::string(String::new())),
            "BOOL" | "BOOLEAN" => Ok(Value::bool_val(false)),
            "DATE" => Ok(Value::date(
                chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            )),
            "DATETIME" | "TIMESTAMP" => Ok(Value::timestamp(Utc.timestamp_opt(0, 0).unwrap())),
            "UUID" => Ok(Value::uuid(uuid::Uuid::nil())),
            _ if type_str.starts_with("DECIMAL") => Ok(Value::numeric(rust_decimal::Decimal::ZERO)),
            _ if type_str.starts_with("ARRAY") => Ok(Value::array(vec![])),
            _ => Ok(Value::null()),
        }
    }

    fn eval_get_setting(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "getSetting requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let setting_name = val.to_string();

        match setting_name.as_str() {
            "max_threads" => Ok(Value::string("1".to_string())),
            "max_memory_usage" => Ok(Value::string("10000000000".to_string())),
            "max_block_size" => Ok(Value::string("65536".to_string())),
            _ => Ok(Value::string("0".to_string())),
        }
    }

    fn eval_is_decimal_overflow(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "isDecimalOverflow requires 1 argument".to_string(),
            ));
        }
        let _val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        Ok(Value::bool_val(false))
    }

    fn eval_count_digits(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "countDigits requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }

        let count = if let Some(n) = val.as_i64() {
            n.abs().to_string().len() as i64
        } else if let Some(f) = val.as_f64() {
            let s = f.abs().to_string();
            s.chars().filter(|c| c.is_ascii_digit()).count() as i64
        } else if let Some(d) = val.as_numeric() {
            let s = d.abs().to_string();
            s.chars().filter(|c| c.is_ascii_digit()).count() as i64
        } else if let Some(s) = val.as_str() {
            s.chars().filter(|c| c.is_ascii_digit()).count() as i64
        } else {
            0
        };
        Ok(Value::int64(count))
    }
}
