#[cfg(test)]
#[allow(clippy::approx_constant)]
mod value_size_tests {
    use std::mem;

    use chrono::{NaiveDate, NaiveTime, Utc};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    use crate::types::Value;

    #[test]
    fn test_value_size() {
        let value_size = mem::size_of::<Value>();
        assert!(value_size <= 72, "Value enum should not exceed 72 bytes");

        let variants = vec![
            ("Null", Value::null()),
            ("Bool(true)", Value::bool_val(true)),
            ("Int64(42)", Value::int64(42)),
            ("Float64(3.14)", Value::float64(3.14)),
            ("Numeric(Decimal)", Value::numeric(Decimal::new(100, 2))),
            ("String(empty)", Value::string(String::new())),
            ("String(3 chars)", Value::string("abc".to_string())),
            (
                "String(15 chars)",
                Value::string("123456789012345".to_string()),
            ),
            ("String(100 chars)", Value::string("a".repeat(100))),
            ("Bytes(empty)", Value::bytes(vec![])),
            ("Bytes(10)", Value::bytes(vec![0; 10])),
            (
                "Date",
                Value::date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            ),
            (
                "Time",
                Value::time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
            ),
            ("Timestamp", Value::timestamp(Utc::now())),
            ("DateTime", Value::datetime(Utc::now())),
            ("Uuid", Value::uuid(Uuid::new_v4())),
            ("Array(empty)", Value::array(vec![])),
            (
                "Array(3 ints)",
                Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)]),
            ),
            ("Json(null)", Value::json(serde_json::Value::Null)),
            ("Default", Value::default_value()),
        ];

        for (_name, _value) in variants {
            assert_eq!(mem::size_of_val(&_value), value_size);
        }
    }

    #[test]
    fn test_proposed_small_value() {
        #[repr(C)]
        struct SmallValue {
            tag: u8,
            data: [u8; 15],
        }

        let small_size = mem::size_of::<SmallValue>();
        assert_eq!(small_size, 16, "SmallValue should be exactly 16 bytes");
    }

    #[test]
    fn test_box_pointer_size() {
        let box_size = mem::size_of::<Box<String>>();
        assert_eq!(box_size, 8, "Box pointer should be 8 bytes on 64-bit");
    }
}
