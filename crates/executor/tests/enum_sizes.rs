#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use std::mem::size_of;

use debug_print::debug_eprintln;
use yachtsql::{DataType, Error, Value};

struct SizeThresholds {
    excellent: usize,
    good: usize,
    warning: usize,
    critical: usize,
}

impl SizeThresholds {
    const HOT_PATH: Self = Self {
        excellent: 32,
        good: 64,
        warning: 128,
        critical: 128,
    };

    const FREQUENT: Self = Self {
        excellent: 64,
        good: 128,
        warning: 256,
        critical: 256,
    };

    const LARGE: Self = Self {
        excellent: 128,
        good: 256,
        warning: 512,
        critical: 512,
    };

    fn assess(&self, size: usize) -> SizeAssessment {
        if size <= self.excellent {
            SizeAssessment::Excellent
        } else if size <= self.good {
            SizeAssessment::Good
        } else if size <= self.warning {
            SizeAssessment::Warning
        } else {
            SizeAssessment::Critical
        }
    }

    fn format_assessment(&self, name: &str, size: usize) -> String {
        match self.assess(size) {
            SizeAssessment::Excellent => {
                format!("✅ {:<10} {:>4} bytes - Excellent (small)", name, size)
            }
            SizeAssessment::Good => {
                format!("✅ {:<10} {:>4} bytes - Good (acceptable)", name, size)
            }
            SizeAssessment::Warning => {
                format!(
                    "⚠️  {:<10} {:>4} bytes - Large (watch for regressions)",
                    name, size
                )
            }
            SizeAssessment::Critical => {
                format!(
                    "🔴 {:<10} {:>4} bytes - Very Large (consider optimization)",
                    name, size
                )
            }
        }
    }

    fn check_critical(&self, name: &str, size: usize) {
        assert!(
            size <= self.critical,
            "❌ {} enum is {} bytes (limit: {}) - optimization required",
            name,
            size,
            self.critical
        );
    }
}

enum SizeAssessment {
    Excellent,
    Good,
    Warning,
    Critical,
}

fn document_enum_size<T>(name: &str) -> usize {
    let size = size_of::<T>();
    debug_eprintln!("[test::enum_sizes] {:<10} {:>4} bytes", name, size);
    size
}

fn analyze_enum_size(name: &str, size: usize, thresholds: &SizeThresholds) {
    debug_eprintln!(
        "[test::enum_sizes] {}",
        thresholds.format_assessment(name, size)
    );
}

#[test]
fn document_public_enum_sizes() {
    debug_eprintln!("[test::enum_sizes] \n=== Public Enum Memory Layout ===\n");

    let value_size = document_enum_size::<Value>("Value:");
    let datatype_size = document_enum_size::<DataType>("DataType:");
    let error_size = document_enum_size::<Error>("Error:");

    debug_eprintln!("[test::enum_sizes] \n=== Size Analysis ===\n");
    analyze_enum_size("Value", value_size, &SizeThresholds::HOT_PATH);
    analyze_enum_size("DataType", datatype_size, &SizeThresholds::HOT_PATH);
    analyze_enum_size("Error", error_size, &SizeThresholds::FREQUENT);

    debug_eprintln!("[test::enum_sizes] \n=== Regression Alerts ===\n");
    SizeThresholds::HOT_PATH.check_critical("Value", value_size);
    SizeThresholds::HOT_PATH.check_critical("DataType", datatype_size);
    SizeThresholds::FREQUENT.check_critical("Error", error_size);

    debug_eprintln!("[test::enum_sizes] All public enum sizes within acceptable limits\n");
}

#[test]
fn document_value_variant_sizes() {
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use indexmap::IndexMap;
    use rust_decimal::Decimal;

    debug_eprintln!("[test::enum_sizes] \n=== Value Enum Variant Sizes ===\n");

    let print_category = |category: &str| {
        debug_eprintln!("[test::enum_sizes] {}:", category);
    };

    let print_size = |name: &str, size: usize| {
        debug_eprintln!("[test::enum_sizes]   {:<18} {:>3} bytes", name, size);
    };

    print_category("Small variants (1-8 bytes)");
    print_size("bool", size_of::<bool>());
    print_size("i64", size_of::<i64>());
    print_size("f64", size_of::<f64>());
    print_size("Decimal", size_of::<Decimal>());
    debug_eprintln!("[test::enum_sizes]");

    print_category("Medium variants (16-32 bytes)");
    print_size("NaiveDate", size_of::<NaiveDate>());
    print_size("NaiveTime", size_of::<NaiveTime>());
    print_size("DateTime<Utc>", size_of::<DateTime<Utc>>());
    debug_eprintln!("[test::enum_sizes]");

    print_category("Large variants (24+ bytes)");
    print_size("String", size_of::<String>());
    print_size("Vec<u8>", size_of::<Vec<u8>>());
    print_size("Vec<Value>", size_of::<Vec<Value>>());
    print_size("IndexMap", size_of::<IndexMap<String, Value>>());
    print_size("serde_json::Value", size_of::<serde_json::Value>());
    debug_eprintln!("[test::enum_sizes]");

    debug_eprintln!(
        "[test::enum_sizes] Note: Value enum size is determined by the LARGEST variant"
    );
    debug_eprintln!(
        "[test::enum_sizes] plus discriminant (typically 8 bytes on 64-bit systems).\n"
    );
}

#[test]
fn track_size_changes_over_time() {
    let value_size = size_of::<Value>();
    let datatype_size = size_of::<DataType>();
    let error_size = size_of::<Error>();

    fn warn_if_oversized(name: &str, current_size: usize, limit: usize, suggestions: &[&str]) {
        if current_size > limit {
            debug_eprintln!(
                "[test::enum_sizes] \nWARNING: {} enum has grown to {} bytes",
                name,
                current_size
            );
            debug_eprintln!("[test::enum_sizes]    Previous limit: {} bytes", limit);
            debug_eprintln!("[test::enum_sizes]    Consider boxing large variants:");
            for suggestion in suggestions {
                debug_eprintln!("[test::enum_sizes]    - {}", suggestion);
            }
            debug_eprintln!("[test::enum_sizes]");
        }
    }

    warn_if_oversized(
        "Value",
        value_size,
        64,
        &[
            "String(Box<String>)",
            "Array(Box<Vec<Value>>)",
            "Struct(Box<IndexMap<String, Value>>)",
            "Json(Box<serde_json::Value>)",
        ],
    );

    warn_if_oversized(
        "DataType",
        datatype_size,
        64,
        &["Consider boxing large type variants"],
    );

    warn_if_oversized(
        "Error",
        error_size,
        128,
        &["Consider boxing large error variants"],
    );
}
