use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::super::IrEvaluator;

impl<'a> IrEvaluator<'a> {
    pub(crate) fn fn_upper(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::String(s.to_uppercase())),
            _ => Err(Error::InvalidQuery("UPPER requires string argument".into())),
        }
    }

    pub(crate) fn fn_lower(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::String(s.to_lowercase())),
            _ => Err(Error::InvalidQuery("LOWER requires string argument".into())),
        }
    }

    pub(crate) fn fn_length(&self, args: &[Value]) -> Result<Value> {
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

    pub(crate) fn fn_concat(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::String(String::new()));
        }
        let first = &args[0];
        let is_bytes = matches!(first, Value::Bytes(_));

        if is_bytes {
            let mut result: Vec<u8> = Vec::new();
            for arg in args {
                match arg {
                    Value::Null => return Ok(Value::Null),
                    Value::Bytes(b) => result.extend(b),
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
                    | Value::Default => {
                        return Err(Error::InvalidQuery(
                            "CONCAT with BYTES requires all arguments to be BYTES".into(),
                        ));
                    }
                }
            }
            Ok(Value::Bytes(result))
        } else {
            let mut result = String::new();
            for arg in args {
                match arg {
                    Value::Null => return Ok(Value::Null),
                    Value::String(s) => result.push_str(s),
                    Value::Bool(_)
                    | Value::Int64(_)
                    | Value::Float64(_)
                    | Value::Numeric(_)
                    | Value::BigNumeric(_)
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
                    | Value::Default => {
                        return Err(Error::InvalidQuery(
                            "CONCAT requires STRING arguments".into(),
                        ));
                    }
                }
            }
            Ok(Value::String(result))
        }
    }

    pub(crate) fn fn_trim(&self, args: &[Value]) -> Result<Value> {
        match args {
            [Value::Null, ..] => Ok(Value::Null),
            [Value::String(s)] => Ok(Value::String(s.trim().to_string())),
            [Value::String(s), Value::String(chars)] => {
                let char_set: std::collections::HashSet<char> = chars.chars().collect();
                let result = s
                    .trim_start_matches(|c| char_set.contains(&c))
                    .trim_end_matches(|c| char_set.contains(&c))
                    .to_string();
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery("TRIM requires string argument".into())),
        }
    }

    pub(crate) fn fn_ltrim(&self, args: &[Value]) -> Result<Value> {
        match args {
            [Value::Null, ..] => Ok(Value::Null),
            [Value::String(s)] => Ok(Value::String(s.trim_start().to_string())),
            [Value::String(s), Value::String(chars)] => {
                let char_set: std::collections::HashSet<char> = chars.chars().collect();
                let result = s.trim_start_matches(|c| char_set.contains(&c)).to_string();
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery("LTRIM requires string argument".into())),
        }
    }

    pub(crate) fn fn_rtrim(&self, args: &[Value]) -> Result<Value> {
        match args {
            [Value::Null, ..] => Ok(Value::Null),
            [Value::String(s)] => Ok(Value::String(s.trim_end().to_string())),
            [Value::String(s), Value::String(chars)] => {
                let char_set: std::collections::HashSet<char> = chars.chars().collect();
                let result = s.trim_end_matches(|c| char_set.contains(&c)).to_string();
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery("RTRIM requires string argument".into())),
        }
    }

    pub(crate) fn fn_substr(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "SUBSTR requires at least 1 argument".into(),
            ));
        }
        let s = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::String(s) => s,
            _ => {
                return Err(Error::InvalidQuery(
                    "SUBSTR requires string argument".into(),
                ));
            }
        };
        let start_raw = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1);
        let len = args.get(2).and_then(|v| v.as_i64()).map(|l| l as usize);
        let chars: Vec<char> = s.chars().collect();
        let char_len = chars.len();

        let start_idx = if start_raw < 0 {
            char_len.saturating_sub((-start_raw) as usize)
        } else if start_raw == 0 {
            0
        } else {
            (start_raw as usize).saturating_sub(1).min(char_len)
        };

        let end_idx = len
            .map(|l| (start_idx + l).min(char_len))
            .unwrap_or(char_len);
        Ok(Value::String(chars[start_idx..end_idx].iter().collect()))
    }

    pub(crate) fn fn_replace(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery("REPLACE requires 3 arguments".into()));
        }
        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) => Ok(Value::Null),
            (Value::String(s), Value::String(from), Value::String(to)) => {
                if from.is_empty() {
                    Ok(Value::String(s.clone()))
                } else {
                    Ok(Value::String(s.replace(from.as_str(), to.as_str())))
                }
            }
            _ => Err(Error::InvalidQuery(
                "REPLACE requires string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_reverse(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::String(s.chars().rev().collect())),
            Some(Value::Bytes(b)) => Ok(Value::Bytes(b.iter().rev().cloned().collect())),
            Some(Value::Array(a)) => Ok(Value::Array(a.iter().rev().cloned().collect())),
            _ => Err(Error::InvalidQuery(
                "REVERSE requires string/bytes/array argument".into(),
            )),
        }
    }

    pub(crate) fn fn_left(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("LEFT requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::Int64(n)) => {
                let chars: Vec<char> = s.chars().collect();
                let n = (*n as usize).min(chars.len());
                Ok(Value::String(chars[..n].iter().collect()))
            }
            (Value::Bytes(b), Value::Int64(n)) => {
                let n = (*n as usize).min(b.len());
                Ok(Value::Bytes(b[..n].to_vec()))
            }
            _ => Err(Error::InvalidQuery(
                "LEFT requires string/bytes and int arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_right(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("RIGHT requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::Int64(n)) => {
                let chars: Vec<char> = s.chars().collect();
                let n = (*n as usize).min(chars.len());
                let start = chars.len().saturating_sub(n);
                Ok(Value::String(chars[start..].iter().collect()))
            }
            (Value::Bytes(b), Value::Int64(n)) => {
                let n = (*n as usize).min(b.len());
                let start = b.len().saturating_sub(n);
                Ok(Value::Bytes(b[start..].to_vec()))
            }
            _ => Err(Error::InvalidQuery(
                "RIGHT requires string/bytes and int arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_repeat(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("REPEAT requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::Int64(n)) => Ok(Value::String(s.repeat(*n as usize))),
            _ => Err(Error::InvalidQuery(
                "REPEAT requires string and int arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_starts_with(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "STARTS_WITH requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(prefix)) => {
                Ok(Value::Bool(s.starts_with(prefix.as_str())))
            }
            _ => Err(Error::InvalidQuery(
                "STARTS_WITH requires string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_ends_with(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("ENDS_WITH requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(suffix)) => {
                Ok(Value::Bool(s.ends_with(suffix.as_str())))
            }
            _ => Err(Error::InvalidQuery(
                "ENDS_WITH requires string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_contains(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("CONTAINS requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(substr)) => {
                Ok(Value::Bool(s.contains(substr.as_str())))
            }
            _ => Err(Error::InvalidQuery(
                "CONTAINS requires string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_strpos(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("STRPOS requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(substr)) => Ok(Value::Int64(
                s.find(substr.as_str()).map(|i| i as i64 + 1).unwrap_or(0),
            )),
            _ => Err(Error::InvalidQuery(
                "STRPOS requires string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_lpad(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "LPAD requires at least 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::Int64(n)) => {
                let pad_str = args.get(2).and_then(|v| v.as_str()).unwrap_or(" ");
                let n = *n as usize;
                let s_chars: Vec<char> = s.chars().collect();
                if s_chars.len() >= n {
                    Ok(Value::String(s_chars[..n].iter().collect()))
                } else {
                    let pad_len = n - s_chars.len();
                    let pad_chars: Vec<char> = pad_str.chars().collect();
                    let padded: String = pad_chars
                        .iter()
                        .cycle()
                        .take(pad_len)
                        .chain(s_chars.iter())
                        .collect();
                    Ok(Value::String(padded))
                }
            }
            _ => Err(Error::InvalidQuery(
                "LPAD requires string and int arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_rpad(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "RPAD requires at least 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::Int64(n)) => {
                let pad_str = args.get(2).and_then(|v| v.as_str()).unwrap_or(" ");
                let n = *n as usize;
                let s_chars: Vec<char> = s.chars().collect();
                if s_chars.len() >= n {
                    Ok(Value::String(s_chars[..n].iter().collect()))
                } else {
                    let pad_len = n - s_chars.len();
                    let pad_chars: Vec<char> = pad_str.chars().collect();
                    let padded: String = s_chars
                        .iter()
                        .chain(pad_chars.iter().cycle().take(pad_len))
                        .collect();
                    Ok(Value::String(padded))
                }
            }
            _ => Err(Error::InvalidQuery(
                "RPAD requires string and int arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_initcap(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let delimiters: std::collections::HashSet<char> = args
                    .get(1)
                    .and_then(|v| v.as_str())
                    .map(|d| d.chars().collect())
                    .unwrap_or_else(|| " \t\n\r-_!@#$%^&*()+=[]{}|;:',.<>?/~`".chars().collect());

                let mut result = String::new();
                let mut capitalize_next = true;
                for c in s.chars() {
                    if delimiters.contains(&c) {
                        result.push(c);
                        capitalize_next = true;
                    } else if capitalize_next {
                        result.extend(c.to_uppercase());
                        capitalize_next = false;
                    } else {
                        result.extend(c.to_lowercase());
                    }
                }
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery(
                "INITCAP requires string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_regexp_contains(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "REGEXP_CONTAINS requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(pattern)) => {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;
                Ok(Value::Bool(re.is_match(s)))
            }
            _ => Err(Error::InvalidQuery(
                "REGEXP_CONTAINS requires string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_regexp_replace(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "REGEXP_REPLACE requires 3 arguments".into(),
            ));
        }
        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) => Ok(Value::Null),
            (Value::String(s), Value::String(pattern), Value::String(replacement)) => {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;
                let rust_replacement = replacement
                    .replace("\\1", "$1")
                    .replace("\\2", "$2")
                    .replace("\\3", "$3")
                    .replace("\\4", "$4")
                    .replace("\\5", "$5")
                    .replace("\\6", "$6")
                    .replace("\\7", "$7")
                    .replace("\\8", "$8")
                    .replace("\\9", "$9");
                Ok(Value::String(
                    re.replace_all(s, rust_replacement.as_str()).to_string(),
                ))
            }
            _ => Err(Error::InvalidQuery(
                "REGEXP_REPLACE requires string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_split(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "SPLIT requires at least 1 argument".into(),
            ));
        }
        let delimiter = args.get(1).and_then(|v| v.as_str()).unwrap_or(",");
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(s) => {
                let parts: Vec<Value> = if delimiter.is_empty() {
                    s.chars().map(|c| Value::String(c.to_string())).collect()
                } else {
                    s.split(delimiter)
                        .map(|p| Value::String(p.to_string()))
                        .collect()
                };
                Ok(Value::Array(parts))
            }
            _ => Err(Error::InvalidQuery("SPLIT requires string argument".into())),
        }
    }

    pub(crate) fn fn_instr(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("INSTR requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(substr)) => {
                if substr.is_empty() {
                    return Ok(Value::Int64(0));
                }
                let position = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
                let occurrence = args.get(3).and_then(|v| v.as_i64()).unwrap_or(1) as usize;

                let chars: Vec<char> = s.chars().collect();
                let substr_chars: Vec<char> = substr.chars().collect();
                let char_len = chars.len();

                if position >= 0 {
                    let start_idx = if position == 0 {
                        0
                    } else {
                        (position as usize).saturating_sub(1).min(char_len)
                    };
                    let mut found_count = 0;
                    let mut idx = start_idx;
                    while idx + substr_chars.len() <= char_len {
                        if chars[idx..idx + substr_chars.len()] == substr_chars[..] {
                            found_count += 1;
                            if found_count == occurrence {
                                return Ok(Value::Int64((idx + 1) as i64));
                            }
                        }
                        idx += 1;
                    }
                } else {
                    let start_idx = char_len.saturating_sub((-position) as usize);
                    let mut found_count = 0;
                    let mut idx = start_idx.min(char_len.saturating_sub(substr_chars.len()));
                    loop {
                        if chars[idx..idx + substr_chars.len()] == substr_chars[..] {
                            found_count += 1;
                            if found_count == occurrence {
                                return Ok(Value::Int64((idx + 1) as i64));
                            }
                        }
                        if idx == 0 {
                            break;
                        }
                        idx -= 1;
                    }
                }
                Ok(Value::Int64(0))
            }
            _ => Err(Error::InvalidQuery(
                "INSTR requires string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_format(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "FORMAT requires at least 1 argument".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(fmt) => {
                let format_args = &args[1..];
                let mut arg_index = 0;
                let mut result = String::new();
                let mut chars = fmt.chars().peekable();

                while let Some(c) = chars.next() {
                    if c == '%' {
                        if chars.peek() == Some(&'%') {
                            chars.next();
                            result.push('%');
                            continue;
                        }

                        let mut zero_pad = false;
                        let mut use_grouping = false;
                        let mut width: Option<usize> = None;
                        let mut precision: Option<usize> = None;

                        while let Some(&ch) = chars.peek() {
                            if ch == '0' && width.is_none() {
                                zero_pad = true;
                                chars.next();
                            } else if ch == '\'' {
                                use_grouping = true;
                                chars.next();
                            } else {
                                break;
                            }
                        }

                        let mut width_str = String::new();
                        while let Some(&ch) = chars.peek() {
                            if ch.is_ascii_digit() {
                                width_str.push(ch);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        if !width_str.is_empty() {
                            width = width_str.parse().ok();
                        }

                        if chars.peek() == Some(&'.') {
                            chars.next();
                            let mut prec_str = String::new();
                            while let Some(&ch) = chars.peek() {
                                if ch.is_ascii_digit() {
                                    prec_str.push(ch);
                                    chars.next();
                                } else {
                                    break;
                                }
                            }
                            if !prec_str.is_empty() {
                                precision = prec_str.parse().ok();
                            }
                        }

                        if let Some(&spec) = chars.peek() {
                            chars.next();
                            let val = format_args.get(arg_index);
                            arg_index += 1;

                            let formatted = match spec {
                                's' => val
                                    .map(|v| self.format_value_for_format(v))
                                    .unwrap_or_default(),
                                'd' | 'i' => {
                                    let n = val.and_then(|v| v.as_i64()).unwrap_or(0);
                                    let s = if use_grouping {
                                        Self::format_with_grouping(n)
                                    } else {
                                        n.to_string()
                                    };
                                    if let Some(w) = width {
                                        if zero_pad {
                                            format!("{:0>width$}", s, width = w)
                                        } else {
                                            format!("{:>width$}", s, width = w)
                                        }
                                    } else {
                                        s
                                    }
                                }
                                'f' | 'F' => {
                                    let f = val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                                    let prec = precision.unwrap_or(6);
                                    format!("{:.prec$}", f, prec = prec)
                                }
                                'e' => {
                                    let f = val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                                    let prec = precision.unwrap_or(6);
                                    Self::format_scientific(f, prec, false)
                                }
                                'E' => {
                                    let f = val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                                    let prec = precision.unwrap_or(6);
                                    Self::format_scientific(f, prec, true)
                                }
                                'g' | 'G' => {
                                    let f = val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                                    let prec = precision.unwrap_or(6);
                                    if f.abs() >= 1e-4 && f.abs() < 10_f64.powi(prec as i32) {
                                        format!("{:.prec$}", f, prec = prec)
                                    } else {
                                        Self::format_scientific(f, prec, spec == 'G')
                                    }
                                }
                                'o' => {
                                    let n = val.and_then(|v| v.as_i64()).unwrap_or(0);
                                    format!("{:o}", n)
                                }
                                'x' => {
                                    let n = val.and_then(|v| v.as_i64()).unwrap_or(0);
                                    format!("{:x}", n)
                                }
                                'X' => {
                                    let n = val.and_then(|v| v.as_i64()).unwrap_or(0);
                                    format!("{:X}", n)
                                }
                                't' | 'T' => val
                                    .map(|v| self.format_value_for_format(v))
                                    .unwrap_or_default(),
                                'p' | 'P' => {
                                    let f = val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                                    format!("{}", f * 100.0)
                                }
                                _ => format!("%{}", spec),
                            };
                            result.push_str(&formatted);
                        }
                    } else {
                        result.push(c);
                    }
                }
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery("FORMAT requires string format".into())),
        }
    }

    pub(crate) fn fn_regexp_extract(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "REGEXP_EXTRACT requires 2 arguments".into(),
            ));
        }
        let group_num = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1) as usize;
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(pattern)) => {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;
                match re.captures(s) {
                    Some(caps) => {
                        let matched = caps
                            .get(group_num)
                            .or_else(|| caps.get(0))
                            .map(|m| m.as_str().to_string());
                        Ok(matched.map(Value::String).unwrap_or(Value::Null))
                    }
                    None => Ok(Value::Null),
                }
            }
            _ => Err(Error::InvalidQuery(
                "REGEXP_EXTRACT requires string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_regexp_extract_all(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "REGEXP_EXTRACT_ALL requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(pattern)) => {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;
                let matches: Vec<Value> = re
                    .captures_iter(s)
                    .filter_map(|caps| {
                        caps.get(1)
                            .or_else(|| caps.get(0))
                            .map(|m| Value::String(m.as_str().to_string()))
                    })
                    .collect();
                Ok(Value::Array(matches))
            }
            _ => Err(Error::InvalidQuery(
                "REGEXP_EXTRACT_ALL requires string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_byte_length(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::Int64(s.len() as i64)),
            Some(Value::Bytes(b)) => Ok(Value::Int64(b.len() as i64)),
            _ => Err(Error::InvalidQuery(
                "BYTE_LENGTH requires string or bytes argument".into(),
            )),
        }
    }

    pub(crate) fn fn_ascii(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let code = s.chars().next().map(|c| c as i64).unwrap_or(0);
                Ok(Value::Int64(code))
            }
            _ => Err(Error::InvalidQuery("ASCII requires string argument".into())),
        }
    }

    pub(crate) fn fn_chr(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                if *n == 0 {
                    return Ok(Value::String(String::new()));
                }
                let c = char::from_u32(*n as u32).unwrap_or('\0');
                Ok(Value::String(c.to_string()))
            }
            _ => Err(Error::InvalidQuery("CHR requires integer argument".into())),
        }
    }

    pub(crate) fn fn_unicode(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let code = s.chars().next().map(|c| c as i64).unwrap_or(0);
                Ok(Value::Int64(code))
            }
            _ => Err(Error::InvalidQuery(
                "UNICODE requires string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_to_code_points(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let code_points: Vec<Value> = s.chars().map(|c| Value::Int64(c as i64)).collect();
                Ok(Value::Array(code_points))
            }
            _ => Err(Error::InvalidQuery(
                "TO_CODE_POINTS requires string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_code_points_to_string(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Array(arr)) => {
                let mut result = String::new();
                for v in arr {
                    match v {
                        Value::Null => return Ok(Value::Null),
                        Value::Int64(n) => {
                            if let Some(c) = char::from_u32(*n as u32) {
                                result.push(c);
                            }
                        }
                        _ => {
                            return Err(Error::InvalidQuery(
                                "CODE_POINTS_TO_STRING requires array of integers".into(),
                            ));
                        }
                    }
                }
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery(
                "CODE_POINTS_TO_STRING requires array argument".into(),
            )),
        }
    }

    pub(crate) fn fn_code_points_to_bytes(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Array(arr)) => {
                let mut result = Vec::new();
                for v in arr {
                    match v {
                        Value::Null => return Ok(Value::Null),
                        Value::Int64(n) => {
                            if *n < 0 || *n > 255 {
                                return Err(Error::InvalidQuery(format!(
                                    "CODE_POINTS_TO_BYTES: value {} out of range 0-255",
                                    n
                                )));
                            }
                            result.push(*n as u8);
                        }
                        _ => {
                            return Err(Error::InvalidQuery(
                                "CODE_POINTS_TO_BYTES requires array of integers".into(),
                            ));
                        }
                    }
                }
                Ok(Value::Bytes(result))
            }
            _ => Err(Error::InvalidQuery(
                "CODE_POINTS_TO_BYTES requires array argument".into(),
            )),
        }
    }

    pub(crate) fn fn_translate(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery("TRANSLATE requires 3 arguments".into()));
        }
        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
            (Value::String(source), Value::String(from_chars), Value::String(to_chars)) => {
                let from: Vec<char> = from_chars.chars().collect();
                let to: Vec<char> = to_chars.chars().collect();
                let result: String = source
                    .chars()
                    .filter_map(|c| {
                        if let Some(pos) = from.iter().position(|&fc| fc == c) {
                            to.get(pos).copied()
                        } else {
                            Some(c)
                        }
                    })
                    .collect();
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery(
                "TRANSLATE requires string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_soundex(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                if s.is_empty() {
                    return Ok(Value::String(String::new()));
                }
                let mut result = String::new();
                let mut chars = s.chars().filter(|c| c.is_ascii_alphabetic());
                if let Some(first) = chars.next() {
                    result.push(first.to_ascii_uppercase());
                }
                let get_code = |c: char| -> Option<char> {
                    match c.to_ascii_lowercase() {
                        'b' | 'f' | 'p' | 'v' => Some('1'),
                        'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => Some('2'),
                        'd' | 't' => Some('3'),
                        'l' => Some('4'),
                        'm' | 'n' => Some('5'),
                        'r' => Some('6'),
                        _ => None,
                    }
                };
                let is_hw = |c: char| matches!(c.to_ascii_lowercase(), 'h' | 'w');
                let mut last_code: Option<char> = None;
                for c in chars {
                    if result.len() >= 4 {
                        break;
                    }
                    if is_hw(c) {
                        continue;
                    }
                    if let Some(code) = get_code(c) {
                        if Some(code) != last_code {
                            result.push(code);
                            last_code = Some(code);
                        }
                    } else {
                        last_code = None;
                    }
                }
                while result.len() < 4 {
                    result.push('0');
                }
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery(
                "SOUNDEX requires string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_normalize(&self, args: &[Value]) -> Result<Value> {
        use unicode_normalization::UnicodeNormalization;

        let mode = args
            .get(1)
            .and_then(|v| {
                if let Value::String(s) = v {
                    Some(s.to_uppercase())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "NFC".to_string());

        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let normalized: String = match mode.as_str() {
                    "NFC" => s.nfc().collect(),
                    "NFKC" => s.nfkc().collect(),
                    "NFD" => s.nfd().collect(),
                    "NFKD" => s.nfkd().collect(),
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "Invalid normalization mode: {}. Expected NFC, NFKC, NFD, or NFKD",
                            mode
                        )));
                    }
                };
                Ok(Value::String(normalized))
            }
            _ => Err(Error::InvalidQuery(
                "NORMALIZE requires string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_normalize_and_casefold(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                use unicode_normalization::UnicodeNormalization;
                let normalized: String = s.nfkc().collect();
                let casefolded = normalized.to_lowercase();
                Ok(Value::String(casefolded))
            }
            _ => Err(Error::InvalidQuery(
                "NORMALIZE_AND_CASEFOLD requires string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_edit_distance(&self, args: &[Value]) -> Result<Value> {
        let (s1, s2, max_distance) = match args {
            [Value::Null, ..] | [_, Value::Null, ..] => return Ok(Value::Null),
            [Value::String(a), Value::String(b)] => (a.as_str(), b.as_str(), None),
            [Value::String(a), Value::String(b), Value::Int64(max)] => {
                (a.as_str(), b.as_str(), Some(*max as usize))
            }
            _ => {
                return Err(Error::InvalidQuery(
                    "EDIT_DISTANCE requires two string arguments".into(),
                ));
            }
        };

        let len1 = s1.chars().count();
        let len2 = s2.chars().count();

        if let Some(max) = max_distance {
            if len1.abs_diff(len2) > max {
                return Ok(Value::Int64(max as i64));
            }
        }

        let mut prev_row: Vec<usize> = (0..=len2).collect();
        let mut curr_row = vec![0; len2 + 1];

        for (i, c1) in s1.chars().enumerate() {
            curr_row[0] = i + 1;
            for (j, c2) in s2.chars().enumerate() {
                let cost = if c1 == c2 { 0 } else { 1 };
                curr_row[j + 1] = (prev_row[j + 1] + 1)
                    .min(curr_row[j] + 1)
                    .min(prev_row[j] + cost);
            }
            std::mem::swap(&mut prev_row, &mut curr_row);
        }

        let distance = prev_row[len2];
        let result = match max_distance {
            Some(max) if distance > max => max,
            _ => distance,
        };
        Ok(Value::Int64(result as i64))
    }

    pub(crate) fn fn_contains_substr(&self, args: &[Value]) -> Result<Value> {
        match args {
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [haystack_val, Value::String(needle)] => {
                use unicode_normalization::UnicodeNormalization;
                let haystack = self.value_to_contains_substr_string(haystack_val);
                let normalized_haystack: String = haystack.nfkc().collect();
                let normalized_needle: String = needle.nfkc().collect();
                let result = normalized_haystack
                    .to_lowercase()
                    .contains(&normalized_needle.to_lowercase());
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "CONTAINS_SUBSTR requires string second argument".into(),
            )),
        }
    }

    pub(crate) fn value_to_contains_substr_string(&self, val: &Value) -> String {
        match val {
            Value::Null => "".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int64(n) => n.to_string(),
            Value::Float64(f) => f.to_string(),
            Value::Numeric(d) | Value::BigNumeric(d) => d.to_string(),
            Value::String(s) => s.clone(),
            Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
            Value::Date(d) => d.to_string(),
            Value::Time(t) => t.to_string(),
            Value::DateTime(dt) => dt.to_string(),
            Value::Timestamp(ts) => ts.to_string(),
            Value::Json(j) => j.to_string(),
            Value::Array(arr) => {
                let elements: Vec<String> = arr
                    .iter()
                    .map(|v| self.value_to_contains_substr_string(v))
                    .collect();
                format!("[{}]", elements.join(", "))
            }
            Value::Struct(fields) => {
                let elements: Vec<String> = fields
                    .iter()
                    .map(|(_, v)| self.value_to_contains_substr_string(v))
                    .collect();
                format!("({})", elements.join(", "))
            }
            Value::Geography(g) => g.clone(),
            Value::Interval(i) => format!("{:?}", i),
            Value::Range(r) => format!("{:?}", r),
            Value::Default => "DEFAULT".to_string(),
        }
    }

    pub(crate) fn fn_regexp_instr(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "REGEXP_INSTR requires source and pattern arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(source), Value::String(pattern)) => match regex::Regex::new(pattern) {
                Ok(re) => {
                    if let Some(m) = re.find(source) {
                        Ok(Value::Int64((m.start() + 1) as i64))
                    } else {
                        Ok(Value::Int64(0))
                    }
                }
                Err(_) => Ok(Value::Int64(0)),
            },
            _ => Err(Error::InvalidQuery(
                "REGEXP_INSTR expects string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_regexp_substr(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "REGEXP_SUBSTR requires source and pattern arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(source), Value::String(pattern)) => match regex::Regex::new(pattern) {
                Ok(re) => {
                    if let Some(m) = re.find(source) {
                        Ok(Value::String(m.as_str().to_string()))
                    } else {
                        Ok(Value::Null)
                    }
                }
                Err(_) => Ok(Value::Null),
            },
            _ => Err(Error::InvalidQuery(
                "REGEXP_SUBSTR expects string arguments".into(),
            )),
        }
    }
}
