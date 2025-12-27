use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Utc};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{DateTimeField, Expr, WeekStartDay};
use yachtsql_storage::Record;

use super::super::IrEvaluator;
use super::super::helpers::{
    add_interval_to_date, add_interval_to_datetime, bucket_datetime, date_diff_by_part,
    extract_datetime_field, format_date_with_pattern, format_datetime_with_pattern,
    format_time_with_pattern, negate_interval, parse_date_with_pattern,
    parse_datetime_with_pattern, parse_time_with_pattern, trunc_date, trunc_datetime, trunc_time,
};

impl<'a> IrEvaluator<'a> {
    pub(crate) fn fn_datetime(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "DATETIME requires at least 1 argument".into(),
            ));
        }
        if args.len() == 6 {
            let year = args[0]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("DATETIME year must be int".into()))?;
            let month = args[1]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("DATETIME month must be int".into()))?;
            let day = args[2]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("DATETIME day must be int".into()))?;
            let hour = args[3]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("DATETIME hour must be int".into()))?;
            let minute = args[4]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("DATETIME minute must be int".into()))?;
            let second = args[5]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("DATETIME second must be int".into()))?;
            let dt = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                .and_then(|d| d.and_hms_opt(hour as u32, minute as u32, second as u32))
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime components".into()))?;
            return Ok(Value::DateTime(dt));
        }
        if args.len() == 2 {
            if let (Value::Date(d), Value::Time(t)) = (&args[0], &args[1]) {
                let dt = chrono::NaiveDateTime::new(*d, *t);
                return Ok(Value::DateTime(dt));
            }
        }
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d"))
                    .map_err(|e| Error::InvalidQuery(format!("Invalid datetime: {}", e)))?;
                Ok(Value::DateTime(dt))
            }
            Some(Value::Date(d)) => Ok(Value::DateTime(d.and_hms_opt(0, 0, 0).unwrap())),
            Some(Value::Timestamp(ts)) => Ok(Value::DateTime(ts.naive_utc())),
            _ => Err(Error::InvalidQuery(
                "DATETIME requires date/string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_date(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|e| Error::InvalidQuery(format!("Invalid date: {}", e)))?;
                Ok(Value::Date(date))
            }
            Some(Value::Timestamp(ts)) => Ok(Value::Date(ts.date_naive())),
            Some(Value::DateTime(dt)) => Ok(Value::Date(dt.date())),
            _ => Err(Error::InvalidQuery(
                "DATE requires date/string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_time(&self, args: &[Value]) -> Result<Value> {
        if args.len() == 3 {
            if args.iter().any(|a| a.is_null()) {
                return Ok(Value::Null);
            }
            let hour = args[0]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("TIME hour must be int".into()))?;
            let minute = args[1]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("TIME minute must be int".into()))?;
            let second = args[2]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("TIME second must be int".into()))?;
            let time = NaiveTime::from_hms_opt(hour as u32, minute as u32, second as u32)
                .ok_or_else(|| Error::InvalidQuery("Invalid time components".into()))?;
            return Ok(Value::Time(time));
        }
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let time = NaiveTime::parse_from_str(s, "%H:%M:%S")
                    .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
                    .map_err(|e| Error::InvalidQuery(format!("Invalid time: {}", e)))?;
                Ok(Value::Time(time))
            }
            Some(Value::Timestamp(ts)) => Ok(Value::Time(ts.time())),
            Some(Value::DateTime(dt)) => Ok(Value::Time(dt.time())),
            _ => Err(Error::InvalidQuery(
                "TIME requires time/string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_timestamp(&self, args: &[Value]) -> Result<Value> {
        if args.len() == 2 {
            match (&args[0], &args[1]) {
                (Value::Null, _) | (_, Value::Null) => return Ok(Value::Null),
                (Value::String(s), Value::String(tz_name)) => {
                    let ndt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                        .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp: {}", e)))?;
                    let tz: chrono_tz::Tz = tz_name.parse().map_err(|_| {
                        Error::InvalidQuery(format!("Invalid timezone: {}", tz_name))
                    })?;
                    let local_dt = ndt.and_local_timezone(tz).single().ok_or_else(|| {
                        Error::InvalidQuery("Ambiguous or invalid local time".into())
                    })?;
                    return Ok(Value::Timestamp(local_dt.with_timezone(&Utc)));
                }
                _ => {
                    return Err(Error::InvalidQuery(
                        "TIMESTAMP with timezone requires (string, string) arguments".into(),
                    ));
                }
            }
        }
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let normalized = if s.ends_with("+00") || s.ends_with("-00") {
                    format!("{}:00", s)
                } else {
                    s.to_string()
                };
                let dt = DateTime::parse_from_rfc3339(&normalized)
                    .map(|d| d.with_timezone(&Utc))
                    .or_else(|_| {
                        DateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%:z")
                            .map(|d| d.with_timezone(&Utc))
                    })
                    .or_else(|_| {
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                            .or_else(|_| {
                                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                            })
                            .map(|ndt| ndt.and_utc())
                    })
                    .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp: {}", e)))?;
                Ok(Value::Timestamp(dt))
            }
            Some(Value::Date(d)) => {
                let ndt = d.and_hms_opt(0, 0, 0).ok_or_else(|| {
                    Error::InvalidQuery("Failed to create timestamp from date".into())
                })?;
                Ok(Value::Timestamp(ndt.and_utc()))
            }
            Some(Value::DateTime(dt)) => Ok(Value::Timestamp(dt.and_utc())),
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP requires string/date argument".into(),
            )),
        }
    }

    pub(crate) fn fn_current_date(&self, _args: &[Value]) -> Result<Value> {
        Ok(Value::Date(Utc::now().date_naive()))
    }

    pub(crate) fn fn_current_timestamp(&self, _args: &[Value]) -> Result<Value> {
        Ok(Value::Timestamp(Utc::now()))
    }

    pub(crate) fn fn_current_time(&self, _args: &[Value]) -> Result<Value> {
        Ok(Value::Time(Utc::now().time()))
    }

    pub(crate) fn fn_current_datetime(&self, _args: &[Value]) -> Result<Value> {
        Ok(Value::DateTime(Utc::now().naive_utc()))
    }

    pub(crate) fn fn_date_add(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("DATE_ADD requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Date(d), Value::Interval(interval)) => {
                let new_date = add_interval_to_date(d, interval)?;
                Ok(Value::Date(new_date))
            }
            (Value::DateTime(dt), Value::Interval(interval)) => {
                let new_dt = add_interval_to_datetime(dt, interval)?;
                Ok(Value::DateTime(new_dt))
            }
            (Value::Timestamp(ts), Value::Interval(interval)) => {
                let new_dt = add_interval_to_datetime(&ts.naive_utc(), interval)?;
                Ok(Value::Timestamp(new_dt.and_utc()))
            }
            _ => Err(Error::InvalidQuery(
                "DATE_ADD requires date/datetime/timestamp and interval".into(),
            )),
        }
    }

    pub(crate) fn fn_date_sub(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("DATE_SUB requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
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
            _ => Err(Error::InvalidQuery(
                "DATE_SUB requires date/datetime/timestamp and interval".into(),
            )),
        }
    }

    pub(crate) fn fn_date_diff(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery("DATE_DIFF requires 3 arguments".into()));
        }
        let part = args[2].as_str().unwrap_or("DAY").to_uppercase();
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Date(d1), Value::Date(d2)) => {
                let result = date_diff_by_part(d1, d2, &part)?;
                Ok(Value::Int64(result))
            }
            (Value::DateTime(dt1), Value::DateTime(dt2)) => {
                let d1 = dt1.date();
                let d2 = dt2.date();
                let result = date_diff_by_part(&d1, &d2, &part)?;
                Ok(Value::Int64(result))
            }
            (Value::Timestamp(ts1), Value::Timestamp(ts2)) => {
                let d1 = ts1.naive_utc().date();
                let d2 = ts2.naive_utc().date();
                let result = date_diff_by_part(&d1, &d2, &part)?;
                Ok(Value::Int64(result))
            }
            _ => Err(Error::InvalidQuery(
                "DATE_DIFF requires date/datetime/timestamp arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_date_trunc(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "DATE_TRUNC requires 2 arguments".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Date(d) => {
                let part = args[1].as_str().unwrap_or("DAY").to_uppercase();
                let truncated = trunc_date(d, &part)?;
                Ok(Value::Date(truncated))
            }
            _ => Err(Error::InvalidQuery(
                "DATE_TRUNC requires a date argument".into(),
            )),
        }
    }

    pub(crate) fn fn_date_bucket(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "DATE_BUCKET requires at least 2 arguments".into(),
            ));
        }
        let date = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Date(d) => *d,
            _ => {
                return Err(Error::InvalidQuery(
                    "DATE_BUCKET requires a date as first argument".into(),
                ));
            }
        };
        let interval = match &args[1] {
            Value::Null => return Ok(Value::Null),
            Value::Interval(i) => i,
            _ => {
                return Err(Error::InvalidQuery(
                    "DATE_BUCKET requires an interval as second argument".into(),
                ));
            }
        };
        let origin = if args.len() >= 3 {
            match &args[2] {
                Value::Null => return Ok(Value::Null),
                Value::Date(d) => *d,
                _ => {
                    return Err(Error::InvalidQuery(
                        "DATE_BUCKET origin must be a date".into(),
                    ));
                }
            }
        } else {
            NaiveDate::from_ymd_opt(1950, 1, 1).unwrap()
        };
        if interval.months != 0 {
            let date_months = date.year() * 12 + date.month() as i32 - 1;
            let origin_months = origin.year() * 12 + origin.month() as i32 - 1;
            let diff_months = date_months - origin_months;
            let bucket_months = interval.months;
            let bucket_idx = if diff_months >= 0 {
                diff_months / bucket_months
            } else {
                (diff_months - bucket_months + 1) / bucket_months
            };
            let bucket_start_months = origin_months + bucket_idx * bucket_months;
            let year = bucket_start_months / 12;
            let month = (bucket_start_months % 12) as u32 + 1;
            let bucketed = NaiveDate::from_ymd_opt(year, month, 1).ok_or_else(|| {
                Error::InvalidQuery("Invalid date in DATE_BUCKET calculation".into())
            })?;
            Ok(Value::Date(bucketed))
        } else if interval.days != 0 {
            let diff_days = date.signed_duration_since(origin).num_days();
            let bucket_days = interval.days as i64;
            let bucket_idx = if diff_days >= 0 {
                diff_days / bucket_days
            } else {
                (diff_days - bucket_days + 1) / bucket_days
            };
            let bucketed = origin + chrono::Duration::days(bucket_idx * bucket_days);
            Ok(Value::Date(bucketed))
        } else {
            Err(Error::InvalidQuery(
                "DATE_BUCKET interval must have days or months".into(),
            ))
        }
    }

    pub(crate) fn fn_datetime_trunc(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "DATETIME_TRUNC requires 2 arguments".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::DateTime(dt) => {
                let part = args[1].as_str().unwrap_or("DAY").to_uppercase();
                let truncated = trunc_datetime(dt, &part)?;
                Ok(Value::DateTime(truncated))
            }
            _ => Err(Error::InvalidQuery(
                "DATETIME_TRUNC requires a datetime argument".into(),
            )),
        }
    }

    pub(crate) fn fn_timestamp_trunc(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIMESTAMP_TRUNC requires 2 arguments".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Timestamp(ts) => {
                let part = args[1].as_str().unwrap_or("DAY").to_uppercase();
                let truncated = trunc_datetime(&ts.naive_utc(), &part)?;
                Ok(Value::Timestamp(truncated.and_utc()))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_TRUNC requires a timestamp argument".into(),
            )),
        }
    }

    pub(crate) fn fn_time_trunc(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIME_TRUNC requires 2 arguments".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Time(t) => {
                let part = args[1].as_str().unwrap_or("SECOND").to_uppercase();
                let truncated = trunc_time(t, &part)?;
                Ok(Value::Time(truncated))
            }
            _ => Err(Error::InvalidQuery(
                "TIME_TRUNC requires a time argument".into(),
            )),
        }
    }

    pub(crate) fn fn_date_from_unix_date(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(days)) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let date = epoch + chrono::Duration::days(*days);
                Ok(Value::Date(date))
            }
            _ => Err(Error::InvalidQuery(
                "DATE_FROM_UNIX_DATE requires integer argument".into(),
            )),
        }
    }

    pub(crate) fn fn_unix_date(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Date(d)) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = d.signed_duration_since(epoch).num_days();
                Ok(Value::Int64(days))
            }
            _ => Err(Error::InvalidQuery(
                "UNIX_DATE requires date argument".into(),
            )),
        }
    }

    pub(crate) fn fn_unix_micros(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Timestamp(ts)) => {
                let micros = ts.timestamp_micros();
                Ok(Value::Int64(micros))
            }
            _ => Err(Error::InvalidQuery(
                "UNIX_MICROS requires timestamp argument".into(),
            )),
        }
    }

    pub(crate) fn fn_unix_millis(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Timestamp(ts)) => {
                let millis = ts.timestamp_millis();
                Ok(Value::Int64(millis))
            }
            _ => Err(Error::InvalidQuery(
                "UNIX_MILLIS requires timestamp argument".into(),
            )),
        }
    }

    pub(crate) fn fn_unix_seconds(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Timestamp(ts)) => {
                let secs = ts.timestamp();
                Ok(Value::Int64(secs))
            }
            _ => Err(Error::InvalidQuery(
                "UNIX_SECONDS requires timestamp argument".into(),
            )),
        }
    }

    pub(crate) fn fn_timestamp_micros(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(micros)) => {
                let ts = DateTime::from_timestamp_micros(*micros)
                    .ok_or_else(|| Error::InvalidQuery("Invalid microseconds".into()))?;
                Ok(Value::Timestamp(ts))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_MICROS requires integer argument".into(),
            )),
        }
    }

    pub(crate) fn fn_timestamp_millis(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(millis)) => {
                let ts = DateTime::from_timestamp_millis(*millis)
                    .ok_or_else(|| Error::InvalidQuery("Invalid milliseconds".into()))?;
                Ok(Value::Timestamp(ts))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_MILLIS requires integer argument".into(),
            )),
        }
    }

    pub(crate) fn fn_timestamp_seconds(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(secs)) => {
                let ts = DateTime::from_timestamp(*secs, 0)
                    .ok_or_else(|| Error::InvalidQuery("Invalid seconds".into()))?;
                Ok(Value::Timestamp(ts))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_SECONDS requires integer argument".into(),
            )),
        }
    }

    pub(crate) fn fn_format_date(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "FORMAT_DATE requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::Date(d)) => {
                let formatted = format_date_with_pattern(d, fmt)?;
                Ok(Value::String(formatted))
            }
            _ => Err(Error::InvalidQuery(
                "FORMAT_DATE requires format string and date".into(),
            )),
        }
    }

    pub(crate) fn fn_format_datetime(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "FORMAT_DATETIME requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::DateTime(dt)) => {
                let formatted = format_datetime_with_pattern(dt, fmt)?;
                Ok(Value::String(formatted))
            }
            _ => Err(Error::InvalidQuery(
                "FORMAT_DATETIME requires format string and datetime".into(),
            )),
        }
    }

    pub(crate) fn fn_format_timestamp(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "FORMAT_TIMESTAMP requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::Timestamp(ts)) => {
                let formatted = format_datetime_with_pattern(&ts.naive_utc(), fmt)?;
                Ok(Value::String(formatted))
            }
            _ => Err(Error::InvalidQuery(
                "FORMAT_TIMESTAMP requires format string and timestamp".into(),
            )),
        }
    }

    pub(crate) fn fn_format_time(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "FORMAT_TIME requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::Time(t)) => {
                let formatted = format_time_with_pattern(t, fmt)?;
                Ok(Value::String(formatted))
            }
            _ => Err(Error::InvalidQuery(
                "FORMAT_TIME requires format string and time".into(),
            )),
        }
    }

    pub(crate) fn fn_parse_date(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "PARSE_DATE requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::String(s)) => {
                let date = parse_date_with_pattern(s, fmt)?;
                Ok(Value::Date(date))
            }
            _ => Err(Error::InvalidQuery(
                "PARSE_DATE requires format and date strings".into(),
            )),
        }
    }

    pub(crate) fn fn_parse_datetime(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "PARSE_DATETIME requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::String(s)) => {
                let dt = parse_datetime_with_pattern(s, fmt)?;
                Ok(Value::DateTime(dt))
            }
            _ => Err(Error::InvalidQuery(
                "PARSE_DATETIME requires format and datetime strings".into(),
            )),
        }
    }

    pub(crate) fn fn_parse_timestamp(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "PARSE_TIMESTAMP requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::String(s)) => {
                let dt = parse_datetime_with_pattern(s, fmt)?;
                Ok(Value::Timestamp(dt.and_utc()))
            }
            _ => Err(Error::InvalidQuery(
                "PARSE_TIMESTAMP requires format and timestamp strings".into(),
            )),
        }
    }

    pub(crate) fn fn_parse_time(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "PARSE_TIME requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::String(s)) => {
                let time = parse_time_with_pattern(s, fmt)?;
                Ok(Value::Time(time))
            }
            _ => Err(Error::InvalidQuery(
                "PARSE_TIME requires format and time strings".into(),
            )),
        }
    }

    pub(crate) fn fn_last_day(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Date(d)) => {
                let year = d.year();
                let month = d.month();
                let next_month = if month == 12 { 1 } else { month + 1 };
                let next_year = if month == 12 { year + 1 } else { year };
                let first_of_next = NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap();
                let last_day = first_of_next - chrono::Duration::days(1);
                Ok(Value::Date(last_day))
            }
            Some(Value::DateTime(dt)) => {
                let year = dt.date().year();
                let month = dt.date().month();
                let next_month = if month == 12 { 1 } else { month + 1 };
                let next_year = if month == 12 { year + 1 } else { year };
                let first_of_next = NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap();
                let last_day = first_of_next - chrono::Duration::days(1);
                Ok(Value::Date(last_day))
            }
            _ => Err(Error::InvalidQuery(
                "LAST_DAY requires date argument".into(),
            )),
        }
    }

    pub(crate) fn fn_datetime_bucket(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "DATETIME_BUCKET requires at least 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::DateTime(dt), Value::Interval(interval)) => {
                let origin = if args.len() > 2 {
                    match &args[2] {
                        Value::DateTime(o) => *o,
                        _ => NaiveDate::from_ymd_opt(1950, 1, 1)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                    }
                } else {
                    NaiveDate::from_ymd_opt(1950, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                };
                let bucket = bucket_datetime(dt, interval, &origin)?;
                Ok(Value::DateTime(bucket))
            }
            _ => Err(Error::InvalidQuery(
                "DATETIME_BUCKET requires datetime and interval arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_timestamp_bucket(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIMESTAMP_BUCKET requires at least 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Timestamp(ts), Value::Interval(interval)) => {
                let origin = if args.len() > 2 {
                    match &args[2] {
                        Value::Timestamp(o) => o.naive_utc(),
                        _ => NaiveDate::from_ymd_opt(1950, 1, 1)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                    }
                } else {
                    NaiveDate::from_ymd_opt(1950, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                };
                let bucket = bucket_datetime(&ts.naive_utc(), interval, &origin)?;
                Ok(Value::Timestamp(bucket.and_utc()))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_BUCKET requires timestamp and interval arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_extract_from_args(&self, args: &[Expr], record: &Record) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "EXTRACT requires field and value".into(),
            ));
        }
        let val = self.evaluate(&args[1], record)?;
        let field = match &args[0] {
            Expr::Column { name, .. } => name.to_uppercase(),
            _ => return Err(Error::InvalidQuery("EXTRACT requires field name".into())),
        };
        let datetime_field = match field.as_str() {
            "YEAR" => DateTimeField::Year,
            "MONTH" => DateTimeField::Month,
            "DAY" => DateTimeField::Day,
            "HOUR" => DateTimeField::Hour,
            "MINUTE" => DateTimeField::Minute,
            "SECOND" => DateTimeField::Second,
            "DAYOFWEEK" => DateTimeField::DayOfWeek,
            "DAYOFYEAR" => DateTimeField::DayOfYear,
            "QUARTER" => DateTimeField::Quarter,
            "WEEK" => DateTimeField::Week(WeekStartDay::Sunday),
            _ => {
                return Err(Error::InvalidQuery(format!(
                    "Unknown EXTRACT field: {}",
                    field
                )));
            }
        };
        extract_datetime_field(&val, datetime_field)
    }
}
