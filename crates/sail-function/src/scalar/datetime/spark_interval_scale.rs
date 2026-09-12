use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray};
use datafusion::arrow::compute::try_binary;
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Float64Type, Int64Type, IntervalUnit, IntervalYearMonthType,
    TimeUnit,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::types::NativeType;
use datafusion_common::{Result, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::invalid_arg_count_exec_err;

/// Spark scales an ANSI interval by operating on its single stored number -- the MONTHS of a
/// year-month interval or the MICROS of a day-time one -- and rounding HALF_UP, away from zero on
/// a tie (`intervalExpressions.scala:610-623,660-676,690-706`). None of the four expressions reads
/// `spark.sql.ansi.enabled`: they use exact operations, and raise on overflow and on a zero
/// divisor in both modes. DataFusion cannot express either half of that -- its `/` truncates and
/// returns NULL for a zero divisor with ANSI off -- which is why these are UDFs and not a rewrite.
macro_rules! interval_scale_udf {
    ($name:ident, $udf:literal, $spark:literal, $arrow:ty, $result:expr, $integral:expr, $fractional:expr) => {
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
                Ok($result)
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
                let interval = interval.as_primitive::<$arrow>();
                let scaled: PrimitiveArray<$arrow> = match number.data_type() {
                    DataType::Int64 => {
                        try_binary(interval, number.as_primitive::<Int64Type>(), $integral)?
                    }
                    DataType::Float64 => {
                        try_binary(interval, number.as_primitive::<Float64Type>(), $fractional)?
                    }
                    other => return plan_err!("Spark `{}` cannot scale by {other}", $spark),
                };
                let scaled: ArrayRef = Arc::new(scaled.with_data_type($result));
                Ok(ColumnarValue::Array(scaled))
            }

            /// The interval keeps its type; the number collapses to the two shapes Spark
            /// distinguishes -- exact integral arithmetic, or a `Double` rounded HALF_UP. A
            /// DECIMAL goes with the fractional branch.
            fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
                let [interval, number] = arg_types else {
                    return Err(invalid_arg_count_exec_err($spark, (2, 2), arg_types.len()));
                };
                if interval != &$result {
                    return plan_err!("Spark `{}` expects {}", $spark, $result);
                }
                // `ImplicitCastInputTypes` with `NumericType`, so a STRING scales the interval
                // too: `NumericType.defaultConcreteType` is DOUBLE (`TypeCoercion.scala:212`).
                let native: NativeType = number.into();
                let number = if native.is_integer() {
                    DataType::Int64
                } else if native.is_numeric()
                    || matches!(native, NativeType::Null | NativeType::String)
                {
                    DataType::Float64
                } else {
                    return plan_err!("Spark `{}` cannot scale by {number}", $spark);
                };
                Ok(vec![interval.clone(), number])
            }
        }
    };
}

interval_scale_udf!(
    SparkMultiplyYmInterval,
    "spark_multiply_ym_interval",
    "MultiplyYMInterval",
    IntervalYearMonthType,
    DataType::Interval(IntervalUnit::YearMonth),
    multiply_integral_i32,
    multiply_fractional_i32
);
interval_scale_udf!(
    SparkDivideYmInterval,
    "spark_divide_ym_interval",
    "DivideYMInterval",
    IntervalYearMonthType,
    DataType::Interval(IntervalUnit::YearMonth),
    divide_integral_i32,
    divide_fractional_i32
);
interval_scale_udf!(
    SparkMultiplyDtInterval,
    "spark_multiply_dt_interval",
    "MultiplyDTInterval",
    DurationMicrosecondType,
    DataType::Duration(TimeUnit::Microsecond),
    multiply_integral_i64,
    multiply_fractional_i64
);
interval_scale_udf!(
    SparkDivideDtInterval,
    "spark_divide_dt_interval",
    "DivideDTInterval",
    DurationMicrosecondType,
    DataType::Duration(TimeUnit::Microsecond),
    divide_integral_i64,
    divide_fractional_i64
);

fn overflow() -> ArrowError {
    ArrowError::ComputeError("integer overflow".to_string())
}

fn divided_by_zero() -> ArrowError {
    ArrowError::ComputeError(
        "[INTERVAL_DIVIDED_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead.".to_string(),
    )
}

/// Rounds HALF_UP and checks the range. `f64::round` breaks ties away from zero, which is what
/// Spark's HALF_UP means for a signed count.
fn round_half_up(
    scaled: f64,
    input: f64,
    min: f64,
    max: f64,
) -> std::result::Result<f64, ArrowError> {
    if !scaled.is_finite() {
        return Err(ArrowError::ComputeError(
            "input is infinite or NaN".to_string(),
        ));
    }
    let rounded = scaled.round();
    if rounded < min || rounded > max {
        return Err(ArrowError::ComputeError(format!(
            "rounded value is out of range for input {input} and rounding mode HALF_UP"
        )));
    }
    Ok(rounded)
}

/// Divides exactly and rounds HALF_UP on the remainder, the way Spark's `IntMath`/`LongMath` do.
/// A float would round the wrong way on a tie, so the remainder decides, not the quotient.
fn divide_half_up(numerator: i128, denominator: i128) -> std::result::Result<i128, ArrowError> {
    if denominator == 0 {
        return Err(divided_by_zero());
    }
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    if 2 * remainder.abs() >= denominator.abs() {
        let away = if (numerator < 0) == (denominator < 0) {
            1
        } else {
            -1
        };
        Ok(quotient + away)
    } else {
        Ok(quotient)
    }
}

fn multiply_integral_i32(months: i32, number: i64) -> std::result::Result<i32, ArrowError> {
    i64::from(months)
        .checked_mul(number)
        .and_then(|scaled| i32::try_from(scaled).ok())
        .ok_or_else(overflow)
}

fn multiply_fractional_i32(months: i32, number: f64) -> std::result::Result<i32, ArrowError> {
    let scaled = round_half_up(
        f64::from(months) * number,
        number,
        f64::from(i32::MIN),
        f64::from(i32::MAX),
    )?;
    Ok(scaled as i32)
}

fn divide_integral_i32(months: i32, number: i64) -> std::result::Result<i32, ArrowError> {
    let scaled = divide_half_up(i128::from(months), i128::from(number))?;
    i32::try_from(scaled).map_err(|_| overflow())
}

fn divide_fractional_i32(months: i32, number: f64) -> std::result::Result<i32, ArrowError> {
    if number == 0.0 {
        return Err(divided_by_zero());
    }
    let scaled = round_half_up(
        f64::from(months) / number,
        number,
        f64::from(i32::MIN),
        f64::from(i32::MAX),
    )?;
    Ok(scaled as i32)
}

fn multiply_integral_i64(micros: i64, number: i64) -> std::result::Result<i64, ArrowError> {
    micros.checked_mul(number).ok_or_else(overflow)
}

fn multiply_fractional_i64(micros: i64, number: f64) -> std::result::Result<i64, ArrowError> {
    let scaled = round_half_up(
        micros as f64 * number,
        number,
        i64::MIN as f64,
        i64::MAX as f64,
    )?;
    Ok(scaled as i64)
}

fn divide_integral_i64(micros: i64, number: i64) -> std::result::Result<i64, ArrowError> {
    let scaled = divide_half_up(i128::from(micros), i128::from(number))?;
    i64::try_from(scaled).map_err(|_| overflow())
}

fn divide_fractional_i64(micros: i64, number: f64) -> std::result::Result<i64, ArrowError> {
    if number == 0.0 {
        return Err(divided_by_zero());
    }
    let scaled = round_half_up(
        micros as f64 / number,
        number,
        i64::MIN as f64,
        i64::MAX as f64,
    )?;
    Ok(scaled as i64)
}
