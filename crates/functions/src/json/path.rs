use serde_json::Value as JsonValue;

use super::error::JsonError;

#[derive(Debug, Clone, PartialEq)]
pub struct JsonPath {
    segments: Vec<PathSegment>,
}

#[derive(Debug, Clone, PartialEq)]
enum PathSegment {
    Root,
    Key(String),
    Index(isize),
    ArrayWildcard,
    Filter(FilterPredicate),
    ItemMethod(ItemMethod),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ItemMethod {
    Type,
    Size,
    Length,
    Double,
    String,
    Boolean,
    Number,
    Date,
    Timestamp,
    Abs,
    Floor,
    Ceiling,
}

#[derive(Debug, Clone, PartialEq)]
struct FilterPredicate {
    conditions: Vec<FilterCondition>,
}

#[derive(Debug, Clone, PartialEq)]
struct FilterCondition {
    path_segments: Vec<String>,
    op: FilterOp,
    value: FilterValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
enum FilterValue {
    Number(f64),
    String(String),
    Bool(bool),
    Null,
}

impl JsonPath {
    pub fn parse(path_expr: &str) -> std::result::Result<Self, JsonError> {
        if path_expr.is_empty() {
            return Err(JsonError::InvalidPath("Path cannot be empty".to_string()));
        }

        let mut segments = Vec::new();
        let mut chars = path_expr.chars().peekable();

        if chars.peek() == Some(&'$') {
            segments.push(PathSegment::Root);
            chars.next();
        } else {
            segments.push(PathSegment::Root);
        }

        while let Some(ch) = chars.peek() {
            if ch.is_whitespace() {
                chars.next();
                continue;
            }
            match ch {
                '.' => {
                    chars.next();
                    let field_name = Self::parse_field_name(&mut chars)?;

                    if chars.peek() == Some(&'(') {
                        chars.next();

                        if chars.peek() != Some(&')') {
                            return Err(JsonError::InvalidPath(format!(
                                "Expected ')' after method '{}('",
                                field_name
                            )));
                        }
                        chars.next();

                        let method = Self::parse_item_method(&field_name)?;
                        segments.push(PathSegment::ItemMethod(method));
                    } else {
                        segments.push(PathSegment::Key(field_name));
                    }
                }
                '[' => {
                    chars.next();
                    match chars.peek() {
                        Some(&quote) if quote == '"' || quote == '\'' => {
                            let key = Self::parse_bracket_key(&mut chars)?;
                            segments.push(PathSegment::Key(key));
                        }
                        Some('*') => {
                            chars.next();
                            let closing = chars.next().ok_or_else(|| {
                                JsonError::InvalidPath(
                                    "Unclosed bracket: missing ']' after wildcard".to_string(),
                                )
                            })?;
                            if closing != ']' {
                                return Err(JsonError::InvalidPath(format!(
                                    "Expected ']' after wildcard, found '{}'",
                                    closing
                                )));
                            }
                            segments.push(PathSegment::ArrayWildcard);
                        }
                        Some(_) => {
                            let index = Self::parse_array_index(&mut chars)?;
                            segments.push(PathSegment::Index(index));
                        }
                        None => {
                            return Err(JsonError::InvalidPath(
                                "Unexpected end of input after '['".to_string(),
                            ));
                        }
                    }
                }
                '?' => {
                    let predicate = Self::parse_filter(&mut chars)?;
                    segments.push(PathSegment::Filter(predicate));
                }
                _ => {
                    return Err(JsonError::InvalidPath(format!(
                        "Unexpected character '{}' in path",
                        ch
                    )));
                }
            }
        }

        if segments.is_empty() {
            return Err(JsonError::InvalidPath(
                "Path must contain at least root segment".to_string(),
            ));
        }

        Ok(JsonPath { segments })
    }

    pub fn evaluate(&self, json: &JsonValue) -> std::result::Result<Vec<JsonValue>, JsonError> {
        let mut current: Vec<&JsonValue> = vec![json];
        let mut owned_values: Vec<Vec<JsonValue>> = Vec::new();

        for (idx, segment) in self.segments.iter().enumerate() {
            if matches!(segment, PathSegment::Root) && idx == 0 {
                continue;
            }

            if matches!(segment, PathSegment::ItemMethod(_)) {
                let mut results = Vec::new();
                for value in &current {
                    if let PathSegment::ItemMethod(method) = segment {
                        if let Some(result) = Self::apply_item_method(value, *method) {
                            results.push(result);
                        }
                    }
                }

                if results.is_empty() {
                    return Ok(Vec::new());
                }

                owned_values.push(results);
                current = owned_values.last().unwrap().iter().collect();
                continue;
            }

            let mut next = Vec::new();
            for value in &current {
                match segment {
                    PathSegment::Root => next.push(*value),
                    PathSegment::Key(key) => {
                        if let JsonValue::Object(map) = value
                            && let Some(v) = map.get(key)
                        {
                            next.push(v);
                        }
                    }
                    PathSegment::Index(i) => {
                        if let JsonValue::Array(arr) = value {
                            let idx = if *i < 0 {
                                let len = arr.len() as isize;
                                let pos = len + i;
                                if pos >= 0 { Some(pos as usize) } else { None }
                            } else {
                                Some(*i as usize)
                            };

                            if let Some(idx) = idx {
                                if let Some(v) = arr.get(idx) {
                                    next.push(v);
                                }
                            }
                        }
                    }
                    PathSegment::ArrayWildcard => {
                        if let JsonValue::Array(arr) = value {
                            next.extend(arr.iter());
                        }
                    }
                    PathSegment::Filter(predicate) => match value {
                        JsonValue::Array(arr) => {
                            for element in arr {
                                if predicate.matches(element) {
                                    next.push(element);
                                }
                            }
                        }
                        _ => {
                            if predicate.matches(value) {
                                next.push(*value);
                            }
                        }
                    },
                    PathSegment::ItemMethod(_) => {
                        unreachable!()
                    }
                }
            }

            if next.is_empty() {
                return Ok(Vec::new());
            }

            current = next;
        }

        Ok(current.into_iter().cloned().collect())
    }

    fn parse_field_name(
        chars: &mut std::iter::Peekable<std::str::Chars>,
    ) -> std::result::Result<String, JsonError> {
        let mut field = String::new();
        while let Some(&ch) = chars.peek() {
            if ch == '.' || ch == '[' || ch == '?' || ch == '(' || ch.is_whitespace() {
                break;
            }
            field.push(ch);
            chars.next();
        }

        if field.is_empty() {
            return Err(JsonError::InvalidPath(
                "Field name cannot be empty".to_string(),
            ));
        }

        Ok(field)
    }

    fn parse_bracket_key(
        chars: &mut std::iter::Peekable<std::str::Chars>,
    ) -> std::result::Result<String, JsonError> {
        let quote = chars.next().ok_or_else(|| {
            JsonError::InvalidPath("Expected quote in bracket notation".to_string())
        })?;

        if quote != '"' && quote != '\'' {
            return Err(JsonError::InvalidPath(format!(
                "Expected quote but found '{}'",
                quote
            )));
        }

        let mut key = String::new();
        let mut escaped = false;

        loop {
            let ch = chars.next().ok_or_else(|| {
                JsonError::InvalidPath(format!(
                    "Unclosed bracket: missing matching quote '{}'",
                    quote
                ))
            })?;

            if escaped {
                Self::process_escape_sequence(ch, &mut key);
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote {
                break;
            } else {
                key.push(ch);
            }
        }

        let closing = chars
            .next()
            .ok_or_else(|| JsonError::InvalidPath("Missing closing ']'".to_string()))?;
        if closing != ']' {
            return Err(JsonError::InvalidPath(format!(
                "Expected ']' after quoted key, got '{}'",
                closing
            )));
        }

        Ok(key)
    }

    fn parse_array_index(
        chars: &mut std::iter::Peekable<std::str::Chars>,
    ) -> std::result::Result<isize, JsonError> {
        let mut digits = String::new();
        let mut closed = false;

        if chars.peek() == Some(&'-') {
            digits.push('-');
            chars.next();
        }

        while let Some(&ch) = chars.peek() {
            if ch == ']' {
                chars.next();
                closed = true;
                break;
            }
            if !ch.is_ascii_digit() {
                return Err(JsonError::InvalidPath(format!(
                    "Invalid character '{}' in array index",
                    ch
                )));
            }
            digits.push(ch);
            chars.next();
        }

        if digits.is_empty() || digits == "-" {
            return Err(JsonError::InvalidPath(
                "Array index cannot be empty".to_string(),
            ));
        }
        if !closed {
            return Err(JsonError::InvalidPath(
                "Missing closing ']' after array index".to_string(),
            ));
        }

        digits.parse::<isize>().map_err(|e| {
            JsonError::InvalidPath(format!("Failed to parse array index '{}': {}", digits, e))
        })
    }

    fn parse_filter(
        chars: &mut std::iter::Peekable<std::str::Chars>,
    ) -> std::result::Result<FilterPredicate, JsonError> {
        let Some('?') = chars.next() else {
            return Err(JsonError::InvalidPath(
                "Filter expression must start with '?'".to_string(),
            ));
        };

        Self::skip_whitespace(chars);
        let Some('(') = chars.next() else {
            return Err(JsonError::InvalidPath(
                "Expected '(' after '?' in filter expression".to_string(),
            ));
        };

        let mut depth = 1usize;
        let mut expr = String::new();
        for ch in chars.by_ref() {
            match ch {
                '(' => {
                    depth += 1;
                    expr.push(ch);
                }
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                    expr.push(ch);
                }
                _ => expr.push(ch),
            }
        }

        if depth != 0 {
            return Err(JsonError::InvalidPath(
                "Unbalanced parentheses in filter expression".to_string(),
            ));
        }

        let expression = expr.trim();
        if expression.is_empty() {
            return Err(JsonError::InvalidPath(
                "Filter expression cannot be empty".to_string(),
            ));
        }

        FilterPredicate::parse(expression)
    }

    fn skip_whitespace(chars: &mut std::iter::Peekable<std::str::Chars>) {
        while let Some(ch) = chars.peek() {
            if ch.is_whitespace() {
                chars.next();
            } else {
                break;
            }
        }
    }

    fn process_escape_sequence(ch: char, buffer: &mut String) {
        match ch {
            'n' => buffer.push('\n'),
            't' => buffer.push('\t'),
            'r' => buffer.push('\r'),
            '\\' => buffer.push('\\'),
            '\'' => buffer.push('\''),
            '"' => buffer.push('"'),
            _ => {
                buffer.push('\\');
                buffer.push(ch);
            }
        }
    }

    fn parse_item_method(method_name: &str) -> std::result::Result<ItemMethod, JsonError> {
        match method_name.to_lowercase().as_str() {
            "type" => Ok(ItemMethod::Type),
            "size" => Ok(ItemMethod::Size),
            "length" => Ok(ItemMethod::Length),
            "double" => Ok(ItemMethod::Double),
            "string" => Ok(ItemMethod::String),
            "boolean" => Ok(ItemMethod::Boolean),
            "number" => Ok(ItemMethod::Number),
            "date" => Ok(ItemMethod::Date),
            "timestamp" => Ok(ItemMethod::Timestamp),
            "abs" => Ok(ItemMethod::Abs),
            "floor" => Ok(ItemMethod::Floor),
            "ceiling" => Ok(ItemMethod::Ceiling),
            _ => Err(JsonError::InvalidPath(format!(
                "Unknown item method: '{}'",
                method_name
            ))),
        }
    }

    fn apply_item_method(value: &JsonValue, method: ItemMethod) -> Option<JsonValue> {
        match method {
            ItemMethod::Type => {
                let type_name = match value {
                    JsonValue::Null => "null",
                    JsonValue::Bool(_) => "boolean",
                    JsonValue::Number(_) => "number",
                    JsonValue::String(_) => "string",
                    JsonValue::Array(_) => "array",
                    JsonValue::Object(_) => "object",
                };
                Some(JsonValue::String(type_name.to_string()))
            }
            ItemMethod::Size | ItemMethod::Length => {
                let size = match value {
                    JsonValue::Null => return None,
                    JsonValue::Array(arr) => arr.len() as i64,
                    JsonValue::Object(map) => map.len() as i64,
                    JsonValue::String(s) => s.chars().count() as i64,
                    _ => 1,
                };
                Some(JsonValue::Number(serde_json::Number::from(size)))
            }
            ItemMethod::Double => match value {
                JsonValue::Number(n) => n.as_f64().map(JsonValue::from),
                _ => None,
            },
            ItemMethod::String => match value {
                JsonValue::String(s) => Some(JsonValue::String(s.clone())),
                JsonValue::Number(n) => Some(JsonValue::String(n.to_string())),
                JsonValue::Bool(b) => Some(JsonValue::String(b.to_string())),
                _ => None,
            },
            ItemMethod::Boolean => match value {
                JsonValue::Bool(b) => Some(JsonValue::Bool(*b)),
                _ => None,
            },
            ItemMethod::Number => match value {
                JsonValue::Number(n) => Some(JsonValue::Number(n.clone())),
                _ => None,
            },
            ItemMethod::Date => match value {
                JsonValue::String(_s) => Some(value.clone()),
                _ => None,
            },
            ItemMethod::Timestamp => match value {
                JsonValue::String(_s) => Some(value.clone()),
                _ => None,
            },
            ItemMethod::Abs => match value {
                JsonValue::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Some(JsonValue::Number(serde_json::Number::from(i.abs())))
                    } else {
                        n.as_f64().map(|f| JsonValue::from(f.abs()))
                    }
                }
                _ => None,
            },
            ItemMethod::Floor => match value {
                JsonValue::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        Some(JsonValue::from(f.floor()))
                    } else {
                        Some(value.clone())
                    }
                }
                _ => None,
            },
            ItemMethod::Ceiling => match value {
                JsonValue::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        Some(JsonValue::from(f.ceil()))
                    } else {
                        Some(value.clone())
                    }
                }
                _ => None,
            },
        }
    }
}

impl FilterPredicate {
    fn parse(expression: &str) -> std::result::Result<Self, JsonError> {
        let mut conditions = Vec::new();
        for part in expression.split("&&") {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                return Err(JsonError::InvalidPath(
                    "Filter expression cannot be empty".to_string(),
                ));
            }
            conditions.push(FilterCondition::parse(trimmed)?);
        }

        Ok(FilterPredicate { conditions })
    }

    fn matches(&self, value: &JsonValue) -> bool {
        self.conditions
            .iter()
            .all(|condition| condition.matches(value))
    }
}

impl FilterCondition {
    fn parse(expression: &str) -> std::result::Result<Self, JsonError> {
        let mut operator = None;
        for candidate in ["<=", ">=", "==", "!=", "=", "<", ">"] {
            if expression.contains(candidate) {
                operator = Some(candidate);
                break;
            }
        }

        let op_str = operator.ok_or_else(|| {
            JsonError::InvalidPath(format!(
                "Unsupported filter expression '{}': missing comparison operator",
                expression
            ))
        })?;

        let parts: Vec<&str> = expression.splitn(2, op_str).collect();
        if parts.len() != 2 {
            return Err(JsonError::InvalidPath(format!(
                "Invalid filter expression '{}'",
                expression
            )));
        }

        let left = parts[0].trim();
        let right = parts[1].trim();

        let path_segments = Self::parse_path_segments(left)?;
        let op = Self::parse_operator(op_str);
        let value = Self::parse_value(right)?;

        Ok(FilterCondition {
            path_segments,
            op,
            value,
        })
    }

    fn parse_path_segments(path: &str) -> std::result::Result<Vec<String>, JsonError> {
        let mut trimmed = path.trim();
        if let Some(stripped) = trimmed.strip_prefix('@') {
            trimmed = stripped;
        } else {
            return Err(JsonError::InvalidPath(format!(
                "Filter expression must start with '@', got '{}'",
                path
            )));
        }

        trimmed = trimmed.trim_start_matches('.');
        if trimmed.is_empty() {
            return Err(JsonError::InvalidPath(
                "Filter path must reference a field".to_string(),
            ));
        }

        trimmed
            .split('.')
            .map(|segment| {
                if segment.is_empty() {
                    Err(JsonError::InvalidPath(
                        "Filter path contains empty segment".to_string(),
                    ))
                } else {
                    Ok(segment.to_string())
                }
            })
            .collect()
    }

    fn parse_operator(op: &str) -> FilterOp {
        match op {
            "==" | "=" => FilterOp::Eq,
            "!=" => FilterOp::Ne,
            "<=" => FilterOp::Le,
            "<" => FilterOp::Lt,
            ">=" => FilterOp::Ge,
            ">" => FilterOp::Gt,
            _ => FilterOp::Eq,
        }
    }

    fn parse_value(value: &str) -> std::result::Result<FilterValue, JsonError> {
        let trimmed = value.trim();
        if trimmed.eq_ignore_ascii_case("null") {
            return Ok(FilterValue::Null);
        }
        if trimmed.eq_ignore_ascii_case("true") {
            return Ok(FilterValue::Bool(true));
        }
        if trimmed.eq_ignore_ascii_case("false") {
            return Ok(FilterValue::Bool(false));
        }
        if let Ok(number) = trimmed.parse::<f64>() {
            return Ok(FilterValue::Number(number));
        }
        if let Some(stripped) = trimmed.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
            return Ok(FilterValue::String(stripped.to_string()));
        }
        if let Some(stripped) = trimmed
            .strip_prefix('\'')
            .and_then(|s| s.strip_suffix('\''))
        {
            return Ok(FilterValue::String(stripped.to_string()));
        }

        Err(JsonError::InvalidPath(format!(
            "Unsupported filter literal '{}'",
            value
        )))
    }

    fn matches(&self, value: &JsonValue) -> bool {
        let Some(candidate) = self.extract_path(value) else {
            return false;
        };

        match (&self.value, candidate) {
            (FilterValue::Null, JsonValue::Null) => {
                matches!(self.op, FilterOp::Eq | FilterOp::Le | FilterOp::Ge)
            }
            (FilterValue::Bool(expected), JsonValue::Bool(actual)) => {
                let actual_num = if *actual { 1_f64 } else { 0_f64 };
                let expected_num = if *expected { 1_f64 } else { 0_f64 };
                compare_values(actual_num, expected_num, self.op)
            }
            (FilterValue::Number(expected), JsonValue::Number(actual)) => actual
                .as_f64()
                .map(|actual_num| compare_values(actual_num, *expected, self.op))
                .unwrap_or(false),
            (FilterValue::String(expected), JsonValue::String(actual)) => match self.op {
                FilterOp::Eq => actual == expected,
                FilterOp::Ne => actual != expected,
                FilterOp::Lt => actual < expected,
                FilterOp::Le => actual <= expected,
                FilterOp::Gt => actual > expected,
                FilterOp::Ge => actual >= expected,
            },
            _ => false,
        }
    }

    fn extract_path<'a>(&self, mut value: &'a JsonValue) -> Option<&'a JsonValue> {
        for segment in &self.path_segments {
            match value {
                JsonValue::Object(map) => {
                    value = map.get(segment)?;
                }
                JsonValue::Array(_) => {
                    return None;
                }
                _ => return None,
            }
        }
        Some(value)
    }
}

fn compare_values(actual: f64, expected: f64, op: FilterOp) -> bool {
    match op {
        FilterOp::Eq => (actual - expected).abs() < f64::EPSILON,
        FilterOp::Ne => (actual - expected).abs() >= f64::EPSILON,
        FilterOp::Lt => actual < expected,
        FilterOp::Le => actual <= expected,
        FilterOp::Gt => actual > expected,
        FilterOp::Ge => actual >= expected,
    }
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parse_root_only() {
        let path = JsonPath::parse("$").unwrap();
        assert_eq!(path.segments.len(), 1);
    }

    #[test]
    fn parse_nested_fields() {
        let path = JsonPath::parse("$.user.address.city").unwrap();
        assert_eq!(
            path.segments,
            vec![
                PathSegment::Root,
                PathSegment::Key("user".into()),
                PathSegment::Key("address".into()),
                PathSegment::Key("city".into())
            ]
        );
    }

    #[test]
    fn evaluate_simple_field() {
        let json = json!({"name": "Alice"});
        let path = JsonPath::parse("$.name").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!("Alice")]);
    }

    #[test]
    fn evaluate_array_index() {
        let json = json!([10, 20, 30]);
        let path = JsonPath::parse("$[1]").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(20)]);
    }

    #[test]
    fn wildcard_expands_all_elements() {
        let json = json!({"values": [1, 2, 3]});
        let path = JsonPath::parse("$.values[*]").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(1), json!(2), json!(3)]);
    }

    #[test]
    fn path_not_found_returns_empty() {
        let json = json!({"name": "Alice"});
        let path = JsonPath::parse("$.missing").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_item_method() {
        let path = JsonPath::parse("$.value.type()").unwrap();
        assert_eq!(path.segments.len(), 3);

        match &path.segments[2] {
            PathSegment::ItemMethod(method) => {
                assert_eq!(*method, ItemMethod::Type);
            }
            _ => panic!("Expected ItemMethod, got {:?}", path.segments[2]),
        }
    }

    #[test]
    fn test_item_method_type() {
        let json = json!({"value": 42});
        let path = JsonPath::parse("$.value.type()").unwrap();
        eprintln!(
            "[functions::json::path] Parsed path segments: {:?}",
            path.segments
        );
        let result = path.evaluate(&json).unwrap();
        eprintln!("[functions::json::path] Result: {:?}", result);
        assert_eq!(result, vec![json!("number")]);

        let json = json!({"value": "hello"});
        let path = JsonPath::parse("$.value.type()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!("string")]);

        let json = json!({"value": [1, 2, 3]});
        let path = JsonPath::parse("$.value.type()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!("array")]);
    }

    #[test]
    fn test_item_method_size() {
        let json = json!({"items": [1, 2, 3]});
        let path = JsonPath::parse("$.items.size()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(3)]);

        let json = json!({"obj": {"a": 1, "b": 2}});
        let path = JsonPath::parse("$.obj.size()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(2)]);

        let json = json!({"text": "hello"});
        let path = JsonPath::parse("$.text.size()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(5)]);
    }

    #[test]
    fn test_item_method_double() {
        let json = json!({"value": 42});
        let path = JsonPath::parse("$.value.double()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(42.0)]);

        let json = json!({"value": 3.14});
        let path = JsonPath::parse("$.value.double()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(3.14)]);
    }

    #[test]
    fn test_item_method_abs() {
        let json = json!({"value": -42});
        let path = JsonPath::parse("$.value.abs()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(42)]);

        let json = json!({"value": -3.14});
        let path = JsonPath::parse("$.value.abs()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(3.14)]);
    }

    #[test]
    fn test_negative_array_indexing() {
        let json = json!(["a", "b", "c"]);

        let path = JsonPath::parse("$[-1]").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!("c")]);

        let path = JsonPath::parse("$[-2]").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!("b")]);
    }

    #[test]
    fn test_complex_path_with_item_methods() {
        let json = json!({"user": {"name": "Alice", "scores": [10, 20, 30]}});

        let path = JsonPath::parse("$.user.scores.size()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(3)]);

        let path = JsonPath::parse("$.user.scores[0].double()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(10.0)]);

        let path = JsonPath::parse("$.user.name.type()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!("string")]);
    }

    #[test]
    fn test_floor_and_ceiling() {
        let json = json!({"value": 3.14});

        let path = JsonPath::parse("$.value.floor()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(3.0)]);

        let path = JsonPath::parse("$.value.ceiling()").unwrap();
        let result = path.evaluate(&json).unwrap();
        assert_eq!(result, vec![json!(4.0)]);
    }

    #[test]
    fn test_sql2023_t865_type_method() {
        let test_cases = vec![
            (json!(null), "null"),
            (json!(true), "boolean"),
            (json!(42), "number"),
            (json!("hello"), "string"),
            (json!([1, 2, 3]), "array"),
            (json!({"key": "value"}), "object"),
        ];

        for (input, expected_type) in test_cases {
            let path = JsonPath::parse("$.type()").unwrap();
            let result = path.evaluate(&input).unwrap();
            assert_eq!(
                result,
                vec![json!(expected_type)],
                "Failed for input: {:?}",
                input
            );
        }
    }

    #[test]
    fn test_sql2023_t866_size_method() {
        let json = json!([1, 2, 3, 4, 5]);
        let path = JsonPath::parse("$.size()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!(5)]);

        let json = json!({"a": 1, "b": 2, "c": 3});
        let path = JsonPath::parse("$.size()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!(3)]);

        let json = json!("hello");
        let path = JsonPath::parse("$.size()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!(5)]);

        let json = json!(42);
        let path = JsonPath::parse("$.size()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!(1)]);

        let json = json!(null);
        let path = JsonPath::parse("$.size()").unwrap();
        let empty: Vec<JsonValue> = vec![];
        assert_eq!(path.evaluate(&json).unwrap(), empty);
    }

    #[test]
    fn test_sql2023_t867_double_method() {
        let json = json!({"int": 42, "float": 3.14});

        let path = JsonPath::parse("$.int.double()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!(42.0)]);

        let path = JsonPath::parse("$.float.double()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!(3.14)]);
    }

    #[test]
    fn test_sql2023_t868_string_method() {
        let json = json!({"str": "hello", "num": 123, "bool": true});

        let path = JsonPath::parse("$.str.string()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!("hello")]);

        let path = JsonPath::parse("$.num.string()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!("123")]);

        let path = JsonPath::parse("$.bool.string()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!("true")]);
    }

    #[test]
    fn test_sql2023_negative_indexing() {
        let json = json!(["a", "b", "c", "d", "e"]);

        let path = JsonPath::parse("$[-1]").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!("e")]);

        let path = JsonPath::parse("$[-2]").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!("d")]);

        let path = JsonPath::parse("$[-3]").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!("c")]);
    }

    #[test]
    fn test_sql2023_math_methods() {
        let json = json!({"neg": -42.7, "pos": 3.14});

        let path = JsonPath::parse("$.neg.abs()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!(42.7)]);

        let path = JsonPath::parse("$.pos.floor()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!(3.0)]);

        let path = JsonPath::parse("$.pos.ceiling()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!(4.0)]);
    }

    #[test]
    fn test_nested_paths_with_methods() {
        let json = json!({
            "users": [
                {"name": "Alice", "scores": [10, 20, 30]},
                {"name": "Bob", "scores": [15, 25]}
            ]
        });

        let path = JsonPath::parse("$.users[0].scores.size()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!(3)]);

        let path = JsonPath::parse("$.users[0].name.type()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!("string")]);

        let path = JsonPath::parse("$.users[1].scores[-1].double()").unwrap();
        assert_eq!(path.evaluate(&json).unwrap(), vec![json!(25.0)]);
    }
}
