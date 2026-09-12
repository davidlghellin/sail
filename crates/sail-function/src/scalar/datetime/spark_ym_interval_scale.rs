use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, IntervalYearMonthArray};
use datafusion::arrow::compute::try_binary;
use datafusion::arrow::datatypes::{
    DataType, Float64Type, Int64Type, IntervalUnit, IntervalYearMonthType,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::types::NativeType;
use datafusion_common::{Result, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::invalid_arg_count_exec_err;

/// Spark scales a year-month interval by operating on its MONTHS, an `Int32`, and rounding the
/// result HALF_UP -- away from zero on a tie (`intervalExpressions.scala:615,621,634`). Neither
/// `MultiplyYMInterval` nor `DivideYMInterval` reads `spark.sql.ansi.enabled`: they use exact
/// operations and raise on overflow and on a zero divisor in both modes.
macro_rules! ym_interval_udf {
    ($name:ident, $udf:literal, $spark:literal, $integral:expr, $fractional:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            signature: Signature,
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    signature: Signature::user_defined(Volatility::Immutable),
                }
            }
        }

        impl ScalarUDFImpl for $name {
            fn name(&self) -> &str {
                $udf
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Interval(IntervalUnit::YearMonth))
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let ScalarFunctionArgs {
                    args, number_rows, ..
                } = args;
                if args.len() != 2 {
                    return Err(invalid_arg_count_exec_err($spark, (2, 2), args.len()));
                }
                let interval = args[0].to_array(number_rows)?;
                let number = args[1].to_array(number_rows)?;
                let months = interval.as_primitive::<IntervalYearMonthType>();
                let result: IntervalYearMonthArray = match number.data_type() {
                    DataType::Int64 => {
                        try_binary(months, number.as_primitive::<Int64Type>(), $integral)?
                    }
                    DataType::Float64 => {
                        try_binary(months, number.as_primitive::<Float64Type>(), $fractional)?
                    }
                    other => {
                        return plan_err!("Spark `{}` cannot scale by {other}", $spark);
                    }
                }
                .with_data_type(DataType::Interval(IntervalUnit::YearMonth));
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }

            /// The interval keeps its type; the number collapses to the two shapes Spark
            /// distinguishes -- exact integral arithmetic, or `Double` rounded HALF_UP. A DECIMAL
            /// goes with the fractional branch.
            fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
                let [interval, number] = arg_types else {
                    return Err(invalid_arg_count_exec_err($spark, (2, 2), arg_types.len()));
                };
                if !matches!(interval, DataType::Interval(IntervalUnit::YearMonth)) {
                    return plan_err!("Spark `{}` expects a year-month interval", $spark);
                }
                let native: NativeType = number.into();
                let number = if native.is_integer() {
                    DataType::Int64
                } else if native.is_numeric() || matches!(native, NativeType::Null) {
                    DataType::Float64
                } else {
                    return plan_err!("Spark `{}` cannot scale by {number}", $spark);
                };
                Ok(vec![interval.clone(), number])
            }
        }
    };
}

ym_interval_udf!(
    SparkMultiplyYmInterval,
    "spark_multiply_ym_interval",
    "MultiplyYMInterval",
    multiply_integral,
    multiply_fractional
);
ym_interval_udf!(
    SparkDivideYmInterval,
    "spark_divide_ym_interval",
    "DivideYMInterval",
    divide_integral,
    divide_fractional
);

fn overflow() -> ArrowError {
    ArrowError::ComputeError("integer overflow".to_string())
}

fn divided_by_zero() -> ArrowError {
    ArrowError::ComputeError(
        "[INTERVAL_DIVIDED_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead.".to_string(),
    )
}

/// The months, rounded HALF_UP and checked against the `Int32` a year-month interval is.
fn round_half_up(months: f64, input: f64) -> std::result::Result<i32, ArrowError> {
    if !months.is_finite() {
        return Err(ArrowError::ComputeError(
            "input is infinite or NaN".to_string(),
        ));
    }
    // `f64::round` breaks ties away from zero, which is what Spark's HALF_UP means here.
    let rounded = months.round();
    if rounded < f64::from(i32::MIN) || rounded > f64::from(i32::MAX) {
        return Err(ArrowError::ComputeError(format!(
            "rounded value is out of range for input {input} and rounding mode HALF_UP"
        )));
    }
    Ok(rounded as i32)
}

fn multiply_integral(months: i32, number: i64) -> std::result::Result<i32, ArrowError> {
    i64::from(months)
        .checked_mul(number)
        .and_then(|total| i32::try_from(total).ok())
        .ok_or_else(overflow)
}

fn multiply_fractional(months: i32, number: f64) -> std::result::Result<i32, ArrowError> {
    round_half_up(f64::from(months) * number, number)
}

/// Spark divides the months exactly and rounds HALF_UP (`IntMath.divide`), so the remainder
/// decides, not a float. `i64` cannot overflow here: `|months| <= i32::MAX`.
fn divide_integral(months: i32, number: i64) -> std::result::Result<i32, ArrowError> {
    if number == 0 {
        return Err(divided_by_zero());
    }
    let months = i64::from(months);
    let quotient = months / number;
    let remainder = months % number;
    let rounded = if 2 * remainder.abs() >= number.abs() {
        quotient + if (months < 0) == (number < 0) { 1 } else { -1 }
    } else {
        quotient
    };
    i32::try_from(rounded).map_err(|_| overflow())
}

fn divide_fractional(months: i32, number: f64) -> std::result::Result<i32, ArrowError> {
    if number == 0.0 {
        return Err(divided_by_zero());
    }
    round_half_up(f64::from(months) / number, number)
}
