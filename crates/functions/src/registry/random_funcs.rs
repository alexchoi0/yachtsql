use std::rc::Rc;

use rand::Rng;
use uuid::Uuid;
use yachtsql_common::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "RAND".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RAND".to_string(),
            arg_types: vec![],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |_| {
                let mut rng = rand::thread_rng();
                Ok(Value::float64(rng.r#gen::<f64>()))
            },
        }),
    );

    registry.register_scalar(
        "RANDOM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDOM".to_string(),
            arg_types: vec![],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |_| {
                let mut rng = rand::thread_rng();
                Ok(Value::float64(rng.r#gen::<f64>()))
            },
        }),
    );

    registry.register_scalar(
        "GENERATE_UUID".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "GENERATE_UUID".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: false,
            evaluator: |_| {
                let uuid = Uuid::new_v4();
                Ok(Value::string(uuid.to_string()))
            },
        }),
    );
}
