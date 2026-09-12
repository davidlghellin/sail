use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef, IntervalUnit, TimeUnit, i256};
use datafusion::arrow::error::ArrowError;
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, Operator, ScalarUDF, cast, expr, lit, try_cast,
};
use datafusion_spark::function::math::expr_fn as math_fn;
use half::f16;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::error::generic_exec_err;
use sail_function::scalar::datetime::negate_duration::NegateDuration;
use sail_function::scalar::datetime::spark_interval::SparkDayTimeIntervalToCalendarInterval;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::math::rand_poisson::RandPoisson;
use sail_function::scalar::math::randn::Randn;
use sail_function::scalar::math::random::Random;
use sail_function::scalar::math::spark_abs::SparkAbs;
use sail_function::scalar::math::spark_bin::SparkBin;
use sail_function::scalar::math::spark_bround::SparkBRound;
use sail_function::scalar::math::spark_ceil_floor::{SparkCeil, SparkFloor};
use sail_function::scalar::math::spark_conv::SparkConv;
use sail_function::scalar::math::spark_div::SparkIntervalDiv;
use sail_function::scalar::math::spark_negative::SparkNegative;
use sail_function::scalar::math::spark_pmod::SparkPmod;
use sail_function::scalar::math::spark_signum::SparkSignum;
use sail_function::scalar::math::spark_sqrt::SparkSqrt;
use sail_function::scalar::math::spark_try_add::SparkTryAdd;
use sail_function::scalar::math::spark_try_div::SparkTryDiv;
use sail_function::scalar::math::spark_try_mod::SparkTryMod;
use sail_function::scalar::math::spark_try_mult::SparkTryMult;
use sail_function::scalar::math::spark_try_subtract::SparkTrySubtract;
use sail_function::scalar::math::spark_unhex::SparkUnHex;
use sail_function::scalar::math::spark_uniform::SparkUniform;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};

use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    ScalarFunction, ScalarFunctionInput, is_spark_udt_field, spark_field_type_name, spark_type_name,
};

fn add_day_time_interval_to_string(
    string: Expr,
    interval: Expr,
    string_type: DataType,
    session_timezone: Arc<str>,
    ansi_mode: bool,
) -> PlanResult<Expr> {
    let timestamp = ScalarUDF::from(SparkTimestamp::try_new(
        Some(session_timezone),
        ansi_mode,
        false,
    )?)
    .call(vec![string]);
    let calendar_interval =
        ScalarUDF::from(SparkDayTimeIntervalToCalendarInterval::new()).call(vec![interval]);
    let shifted = timestamp + calendar_interval;
    match string_type {
        DataType::Utf8 => Ok(ScalarUDF::from(SparkToUtf8::new()).call(vec![shifted])),
        DataType::LargeUtf8 => Ok(ScalarUDF::from(SparkToLargeUtf8::new()).call(vec![shifted])),
        DataType::Utf8View => Ok(ScalarUDF::from(SparkToUtf8View::new()).call(vec![shifted])),
        data_type => Err(PlanError::internal(format!(
            "expected string type for interval arithmetic, got {data_type}"
        ))),
    }
}

/// Arguments:
///   - left: A numeric, STRING, DATE, TIMESTAMP, or INTERVAL expression.
///   - right: If left is a numeric right must be numeric expression, or an INTERVAL otherwise.
///
/// Returns:
///   - If left is a numeric, the common maximum type of the arguments.
///   - If one expression is a STRING and the other is a day-time interval, the result is a STRING.
///   - If left is a DATE and right is a day-time interval the result is a TIMESTAMP.
///   - If both expressions are interval they must be of the same class.
///   - Otherwise, the result type matches left.
///
/// Most of the above conditions are handled by DataFusion. Spark-specific coercion differences are
/// rewritten here before constructing the DataFusion expression. For DataFusion's rules, see:
///   https://github.com/apache/datafusion/blob/a28f2834c6969a0c0eb26165031f8baa1e1156a5/datafusion/expr-common/src/type_coercion/binary.rs#L194
fn spark_plus(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if arguments.len() < 2 {
        Ok(arguments.one()?)
    } else {
        let (left, right) = arguments.two()?;
        if let Some(error) = rejects_udt_operand("+", &left, &right, function_context.schema) {
            return Err(error);
        }
        let (left_type, right_type) = (
            left.get_type(function_context.schema),
            right.get_type(function_context.schema),
        );
        if let (Ok(left_type), Ok(right_type)) = (&left_type, &right_type)
            && rejects_add(
                &date_offset_view(&left, left_type, function_context.schema),
                &date_offset_view(&right, right_type, function_context.schema),
                function_context.plan_config.ansi_mode,
            )
        {
            return Err(arithmetic_operand_error("+", left_type, right_type));
        }
        Ok(match (left_type, right_type) {
            (
                Ok(string_type @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)),
                Ok(DataType::Duration(TimeUnit::Microsecond)),
            ) => add_day_time_interval_to_string(
                left,
                right,
                string_type,
                Arc::clone(&function_context.plan_config.session_timezone),
                function_context.plan_config.ansi_mode,
            )?,
            (
                Ok(DataType::Duration(TimeUnit::Microsecond)),
                Ok(string_type @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)),
            ) => add_day_time_interval_to_string(
                right,
                left,
                string_type,
                Arc::clone(&function_context.plan_config.session_timezone),
                function_context.plan_config.ansi_mode,
            )?,
            (Ok(DataType::Date32), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
                left + cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(DataType::Date32)) => {
                cast(left, DataType::Interval(IntervalUnit::MonthDayNano)) + right
            }
            // A Spark day-time interval reaches `+` as Arrow `Duration`, but DataFusion's
            // `time +- interval` rule matches only `Interval(_)`. Spell it the way that rule
            // expects, so the `(Time, IntervalDt)` pair `rejects_add` accepts really resolves
            // (`TimeAddInterval`, `BinaryArithmeticWithDatetimeResolver.scala:87,90`) instead of
            // dying one layer down. The `TIME - TIME` arm in `spark_minus` produces exactly such
            // a `Duration`, so without this the two halves contradict each other.
            // TODO: DataFusion wraps within the 24-hour clock; Spark raises `[DATETIME_OVERFLOW]`
            // in both ANSI modes. `arithmetic_time_subtraction.feature` pins the gap.
            (Ok(DataType::Time32(_) | DataType::Time64(_)), Ok(DataType::Duration(_))) => {
                left + cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Duration(_)), Ok(DataType::Time32(_) | DataType::Time64(_))) => {
                cast(left, DataType::Interval(IntervalUnit::MonthDayNano)) + right
            }
            (Ok(left_type), Ok(DataType::Date32)) if left_type.is_numeric() => {
                cast(left + cast(right, DataType::Int32), DataType::Date32)
            }
            (Ok(DataType::Date32), Ok(right_type)) if right_type.is_numeric() => {
                cast(cast(left, DataType::Int32) + right, DataType::Date32)
            }
            // TODO: In case getting the type fails, we don't want to fail the query.
            //  Future work is needed here, ideally we create something like `Operator::SparkPlus`.
            (Ok(_), Ok(_)) | (Err(_), _) | (_, Err(_)) => left + right,
        })
    }
}

/// Arguments:
///   - left: A numeric, DATE, TIMESTAMP, or INTERVAL expression.
///   - right: The accepted type depends on the type of expr:
///     - If left is a numeric right must be numeric expression.
///     - If left is a year-month or day-time interval, right must be the same class.
///     - Otherwise right must be a DATE or TIMESTAMP.
///
/// Returns:
///   - If left is a numeric, the result is common maximum type of the arguments.
///   - If left is a DATE and right is a day-time interval the result is a TIMESTAMP.
///   - If left is a TIMESTAMP and right is an interval the result is a TIMESTAMP.
///   - If left and right are DATEs the result is an INTERVAL DAYS.
///   - If left or right are TIMESTAMP the result is an INTERVAL DAY TO SECOND.
///   - If both expressions are interval they must be of the same class.
///   - Otherwise, the result type matches left.
///
/// All of the above conditions should be handled by the DataFusion.
/// If there is a discrepancy in parity, check the link below and adjust Sail's logic accordingly:
///   https://github.com/apache/datafusion/blob/a28f2834c6969a0c0eb26165031f8baa1e1156a5/datafusion/expr-common/src/type_coercion/binary.rs#L194
fn spark_minus(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if arguments.len() < 2 {
        let arg = arguments.one()?;
        Ok(spark_unary_negate(
            arg,
            function_context.plan_config.ansi_mode,
            function_context.schema,
        ))
    } else {
        let (left, right) = arguments.two()?;
        if let Some(error) = rejects_udt_operand("-", &left, &right, function_context.schema) {
            return Err(error);
        }
        let (left_type, right_type) = (
            left.get_type(function_context.schema),
            right.get_type(function_context.schema),
        );
        if let (Ok(left_type), Ok(right_type)) = (&left_type, &right_type)
            && rejects_subtract(
                &date_offset_view(&left, left_type, function_context.schema),
                &date_offset_view(&right, right_type, function_context.schema),
                function_context.plan_config.ansi_mode,
            )
        {
            return Err(arithmetic_operand_error("-", left_type, right_type));
        }
        Ok(match (left_type, right_type) {
            // `SubtractTimes` returns a day-time interval (`timeExpressions.scala:632`), but
            // DataFusion coerces a `Time64` pair to `Interval(MonthDayNano)` -- the CALENDAR
            // interval, which combines with nothing day-time. Cast to `Duration` to restore the
            // class Spark gives it.
            // TODO: `Duration` keeps the value and the day-time family but not Spark's
            // `HOUR TO SECOND` start/end fields; `arithmetic_time_subtraction.feature` pins it.
            (
                Ok(DataType::Time32(_) | DataType::Time64(_)),
                Ok(DataType::Time32(_) | DataType::Time64(_)),
            ) => cast(left - right, DataType::Duration(TimeUnit::Microsecond)),
            // The `-` half of the arms above: `TimeAddInterval` with a negated interval
            // (`BinaryArithmeticWithDatetimeResolver.scala:133-134`). `interval - time` is absent
            // on purpose: Spark has no such arm and `rejects_subtract` rejects the pair first.
            (Ok(DataType::Time32(_) | DataType::Time64(_)), Ok(DataType::Duration(_))) => {
                left - cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Date32), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
                left - cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Date32), Ok(right_type)) if right_type.is_numeric() => {
                cast(cast(left, DataType::Int32) - right, DataType::Date32)
            }
            // TODO: In case getting the type fails, we don't want to fail the query.
            //  Future work is needed here, ideally we create something like `Operator::SparkMinus`.
            (Ok(_), Ok(_)) | (Err(_), _) | (_, Err(_)) => left - right,
        })
    }
}

/// Arguments:
///   - left: A numeric or INTERVAL expression.
///   - right: A numeric expression or INTERVAL expression.
///
/// You may not specify an INTERVAL for both arguments.
///
/// Returns:
///   - If both left and right are DECIMAL, the result is DECIMAL.
///   - If left or right is an INTERVAL, the result is of the same type.
///   - If both left and right are integral numeric types, the result is the larger of the two types.
///   - In all other cases the result is a DOUBLE.
///
/// All of the above conditions should be handled by the DataFusion.
/// If there is a discrepancy in parity, check the link below and adjust Sail's logic accordingly:
///   https://github.com/apache/datafusion/blob/a28f2834c6969a0c0eb26165031f8baa1e1156a5/datafusion/expr-common/src/type_coercion/binary.rs#L194
fn spark_multiply(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (left, right) = arguments.two()?;
    if let Some(error) = rejects_udt_operand("*", &left, &right, function_context.schema) {
        return Err(error);
    }
    let (left_type, right_type) = (
        left.get_type(function_context.schema),
        right.get_type(function_context.schema),
    );
    if let (Ok(left_type), Ok(right_type)) = (&left_type, &right_type)
        && rejects_multiply(
            left_type,
            right_type,
            function_context.plan_config.ansi_mode,
        )
    {
        return Err(arithmetic_operand_error("*", left_type, right_type));
    }
    Ok(match (left_type, right_type) {
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(_)) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            cast(
                cast(left, DataType::Int64) * right,
                DataType::Duration(TimeUnit::Microsecond),
            )
        }
        (Ok(_), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            cast(
                left * cast(right, DataType::Int64),
                DataType::Duration(TimeUnit::Microsecond),
            )
        }
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkMultiply`.
        (Ok(_), Ok(_)) | (Err(_), _) | (_, Err(_)) => left * right,
    })
}

/// Check if an expression represents a zero literal value.
/// Handles both direct literals and CAST expressions wrapping literals.
fn is_zero_literal(expr: &Expr) -> bool {
    // Helper to check if a ScalarValue is zero
    fn is_scalar_zero(scalar: &ScalarValue) -> bool {
        match scalar {
            ScalarValue::Int8(Some(0))
            | ScalarValue::Int16(Some(0))
            | ScalarValue::Int32(Some(0))
            | ScalarValue::Int64(Some(0))
            | ScalarValue::UInt8(Some(0))
            | ScalarValue::UInt16(Some(0))
            | ScalarValue::UInt32(Some(0))
            | ScalarValue::UInt64(Some(0))
            | ScalarValue::Decimal128(Some(0), _, _) => true,
            ScalarValue::Float32(Some(v)) if *v == 0.0 => true,
            ScalarValue::Float64(Some(v)) if *v == 0.0 => true,
            ScalarValue::Float16(Some(f)) if *f == f16::from_f32(0.0) => true,
            ScalarValue::Decimal256(Some(v), _, _) if *v == i256::ZERO => true,
            _ => false,
        }
    }

    match expr {
        // Direct literal
        Expr::Literal(scalar, _) => is_scalar_zero(scalar),
        // CAST(literal AS type) - unwrap the cast and check the inner literal
        Expr::Cast(cast_expr) => {
            if let Expr::Literal(scalar, _) = cast_expr.expr.as_ref() {
                is_scalar_zero(scalar)
            } else {
                false
            }
        }
        // TryCast is similar to Cast
        Expr::TryCast(try_cast_expr) => {
            if let Expr::Literal(scalar, _) = try_cast_expr.expr.as_ref() {
                is_scalar_zero(scalar)
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Returns a guarded divisor expression that handles division by zero at runtime.
///
/// In non-ANSI mode: returns `nullif(divisor, 0)` — evaluates to NULL when divisor is zero.
/// In ANSI mode: returns `CASE WHEN divisor = 0 THEN raise_error(msg) ELSE divisor END`.
///
/// This wraps the divisor itself (not the entire division expression) to avoid
/// duplicating complex divisor expressions (e.g., window functions) in the plan.
fn make_safe_divisor(
    divisor: Expr,
    divisor_type: &DataType,
    ansi_mode: bool,
    error_message: &str,
) -> Expr {
    // Skip wrapping for Interval/Duration types (cannot be compared to lit(0)).
    if matches!(divisor_type, DataType::Interval(_) | DataType::Duration(_)) {
        return divisor;
    }

    if ansi_mode {
        let zero_check = divisor.clone().eq(lit(0));
        let raise = Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(RaiseError::new())),
            args: vec![lit(error_message)],
        });
        Expr::Case(expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(zero_check), Box::new(raise))],
            else_expr: Some(Box::new(divisor)),
        })
    } else {
        expr_fn::nullif(divisor, lit(0))
    }
}

/// Arguments:
///   - dividend: A numeric or INTERVAL expression.
///   - divisor: A numeric expression.
///
/// Returns:
///   - If both dividend and divisor are DECIMAL, the result is DECIMAL.
///   - If dividend is a year-month interval, the result is an INTERVAL YEAR TO MONTH.
///   - If dividend is a day-time interval, the result is an INTERVAL DAY TO SECOND.
///   - In all other cases, a DOUBLE.
///
/// All of the above conditions should be handled by the DataFusion.
/// If there is a discrepancy in parity, check the link below and adjust Sail's logic accordingly:
///   https://github.com/apache/datafusion/blob/a28f2834c6969a0c0eb26165031f8baa1e1156a5/datafusion/expr-common/src/type_coercion/binary.rs#L194
fn spark_divide(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (dividend, divisor) = arguments.two()?;
    if let Some(error) = rejects_udt_operand("/", &dividend, &divisor, function_context.schema) {
        return Err(error);
    }

    let ansi_mode = function_context.plan_config.ansi_mode;
    let dividend_type = dividend.get_type(function_context.schema);
    let divisor_type = divisor.get_type(function_context.schema);
    // `Divide.inputType = TypeCollection(DoubleType, DecimalType)` (`arithmetic.scala:812`).
    // Left to DataFusion a non-numeric operand is reinterpreted as its raw integer and yields a
    // meaningless number. A string pair with no numeric operand to anchor the cast is rejected
    // under ANSI on only; under ANSI off both sides coerce to DOUBLE.
    if let (Ok(dividend_type), Ok(divisor_type)) = (&dividend_type, &divisor_type)
        && (rejects_as_divide_dividend(dividend_type)
            || rejects_as_divide_divisor(divisor_type)
            || rejects_unanchored_string_pair(
                operand_role(dividend_type),
                operand_role(divisor_type),
                ansi_mode,
            ))
    {
        return Err(arithmetic_operand_error("/", dividend_type, divisor_type));
    }
    // Plan-time check for literal zero divisors (fast path, better error UX).
    if is_zero_literal(&divisor) {
        if function_context.plan_config.ansi_mode {
            return Err(PlanError::ArrowError(ArrowError::DivideByZero));
        } else {
            return Ok(Expr::Literal(ScalarValue::Null, None));
        }
    }

    // Apply runtime zero-divisor guard to the divisor before building the division expression.
    let effective_divisor_type = divisor_type.as_ref().cloned().unwrap_or(DataType::Int32);
    let divisor = make_safe_divisor(
        divisor,
        &effective_divisor_type,
        ansi_mode,
        "Division by zero",
    );

    let div_expr = match (&dividend_type, &divisor_type) {
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        // TODO: Cast the precision and scale that matches the Spark's behavior after the division.
        //  See `test_divide` in python/pysail/tests/spark/test_math.py
        (Ok(DataType::Decimal128(_, _)), Ok(_))
        | (Ok(_), Ok(DataType::Decimal128(_, _)))
        | (Ok(DataType::Decimal256(_, _)), Ok(_))
        | (Ok(_), Ok(DataType::Decimal256(_, _)))
        | (Ok(DataType::Interval(IntervalUnit::YearMonth)), Ok(_))
        | (Ok(DataType::Interval(IntervalUnit::DayTime)), Ok(_)) => dividend / divisor,
        (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(_)) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            cast(
                cast(dividend, DataType::Int64) / divisor,
                DataType::Duration(TimeUnit::Microsecond),
            )
        }
        (Ok(_), Ok(_)) => cast(dividend, DataType::Float64) / cast(divisor, DataType::Float64),
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkDivide`.
        (Err(_), _) | (_, Err(_)) => dividend / divisor,
    };

    Ok(div_expr)
}

/// Returns the integral part of the division of dividend by divisor.
///
/// Arguments:
///   - dividend: An expression that evaluates to a numeric or interval.
///   - divisor: A matching interval type if dividend is an interval, a numeric otherwise.
///
/// Returns:
///   A BIGINT
///
fn spark_div(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (dividend, divisor) = arguments.two()?;

    // Plan-time check for literal zero divisors.
    if is_zero_literal(&divisor) {
        if function_context.plan_config.ansi_mode {
            return Err(PlanError::ArrowError(ArrowError::DivideByZero));
        } else {
            return Ok(Expr::Literal(ScalarValue::Null, None));
        }
    }

    let ansi_mode = function_context.plan_config.ansi_mode;
    let dividend_type = dividend.get_type(function_context.schema);
    let divisor_type = divisor.get_type(function_context.schema);

    // Apply runtime zero-divisor guard to the divisor before building the division expression.
    let effective_divisor_type = divisor_type.as_ref().cloned().unwrap_or(DataType::Int32);
    let divisor = make_safe_divisor(
        divisor,
        &effective_divisor_type,
        ansi_mode,
        "Division by zero",
    );

    let div_expr = match (&dividend_type, &divisor_type) {
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        (Ok(DataType::Duration(_)), Ok(DataType::Duration(_))) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            cast(dividend, DataType::Int64) / cast(divisor, DataType::Int64)
        }
        // Handle Interval / Interval division using custom UDF
        (Ok(DataType::Interval(_)), Ok(DataType::Interval(_))) => {
            let interval_div = Arc::new(ScalarUDF::from(SparkIntervalDiv::new()));
            Expr::ScalarFunction(expr::ScalarFunction {
                func: interval_div,
                args: vec![dividend, divisor],
            })
        }
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkDivide`.
        (Ok(_), Ok(_)) | (Err(_), _) | (_, Err(_)) => dividend / divisor,
    };

    Ok(cast(div_expr, DataType::Int64))
}

fn power(base: Expr, exponent: Expr) -> Expr {
    cast(expr_fn::power(base, exponent), DataType::Float64)
}

fn hypot(expr1: Expr, expr2: Expr) -> Expr {
    let sum_squared = expr1.clone() * expr1 + expr2.clone() * expr2;
    cast(expr_fn::sqrt(sum_squared), DataType::Float64)
}

fn positive(expr: Expr) -> Expr {
    expr
}

fn rint(expr: Expr) -> Expr {
    cast(expr_fn::round(vec![expr]), DataType::Float64)
}

fn positive_or_null(expr: Expr) -> Expr {
    Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(expr.clone().gt(lit(0_f64))), Box::new(expr))],
        else_expr: None,
    })
}

#[inline]
fn eulers_constant() -> Expr {
    lit(std::f64::consts::E)
}

fn ceil_floor(input: ScalarFunctionInput, name: &str) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let ansi_mode = function_context.plan_config.ansi_mode;
    // DataFusion bug: `ReturnTypeArgs.scalar_arguments` is None if scalar argument is nested
    let arguments = if arguments.len() == 2 {
        let (arg, target_scale) = arguments.two()?;
        let target_scale = match target_scale {
            Expr::Literal(_, _) => Ok(target_scale),
            Expr::Negative(negative) => {
                if let Expr::Literal(scalar, metadata) = *negative {
                    match scalar {
                        ScalarValue::Int8(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -v as i32)),
                            metadata,
                        )),
                        ScalarValue::Int16(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -v as i32)),
                            metadata,
                        )),
                        ScalarValue::Int32(v) => {
                            Ok(Expr::Literal(ScalarValue::Int32(v.map(|v| -v)), metadata))
                        }
                        ScalarValue::Int64(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        ScalarValue::UInt8(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        ScalarValue::UInt16(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        ScalarValue::UInt32(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        ScalarValue::UInt64(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        other => Err(generic_exec_err(
                            "ceil",
                            format!("Target scale must be Integer literal, got {other}").as_str(),
                        )),
                    }
                } else {
                    Err(generic_exec_err(
                        "ceil",
                        format!("Target scale must be Integer literal, got {negative}").as_str(),
                    ))
                }
            }
            _ => Err(generic_exec_err(
                "ceil",
                format!("Target scale must be Integer literal, got {target_scale}").as_str(),
            )),
        }?;
        vec![arg, target_scale]
    } else {
        arguments
    };
    let func = if matches!(name.to_lowercase().trim(), "ceil") {
        Arc::new(ScalarUDF::from(SparkCeil::new(ansi_mode)))
    } else {
        Arc::new(ScalarUDF::from(SparkFloor::new(ansi_mode)))
    };
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func,
        args: arguments,
    }))
}

fn ln(expr: Expr) -> Expr {
    expr_fn::ln(positive_or_null(expr))
}

fn log(base: Expr, num: Expr) -> Expr {
    expr_fn::log(base, positive_or_null(num))
}

fn log10(expr: Expr) -> Expr {
    expr_fn::log10(positive_or_null(expr))
}

fn log1p(expr: Expr) -> Expr {
    expr_fn::ln(positive_or_null(expr + lit(1.0_f64)))
}

fn log2(expr: Expr) -> Expr {
    expr_fn::log2(positive_or_null(expr))
}

fn double(func: impl Fn(Expr) -> Expr) -> impl Fn(Expr) -> Expr {
    move |arg: Expr| func(cast(arg, DataType::Float64))
}

fn double2(func: impl Fn(Expr, Expr) -> Expr) -> impl Fn(Expr, Expr) -> Expr {
    move |arg1: Expr, arg2| func(cast(arg1, DataType::Float64), cast(arg2, DataType::Float64))
}

fn spark_sqrt(arg: Expr) -> Expr {
    ScalarUDF::from(SparkSqrt::new()).call(vec![cast(arg, DataType::Float64)])
}

/// Modulo operation with division-by-zero handling.
///
/// In ANSI mode: raises error for integral/decimal modulo by zero.
/// In non-ANSI mode: returns NULL for modulo by zero.
/// Float/double modulo by zero returns NaN (IEEE 754).
fn spark_modulo(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (dividend, divisor) = arguments.two()?;
    if let Some(error) = rejects_udt_operand("%", &dividend, &divisor, function_context.schema) {
        return Err(error);
    }

    let ansi_mode = function_context.plan_config.ansi_mode;
    let divisor_type = divisor.get_type(function_context.schema);
    // Spark's `%` rejects a non-numeric operand at analysis, and an unanchored string pair
    // only under ANSI on. Left to DataFusion, the rejected pairs reinterpret an operand as a
    // raw integer and compute a meaningless remainder.
    if let (Ok(dividend_type), Ok(divisor_type)) =
        (dividend.get_type(function_context.schema), &divisor_type)
        && (rejects_unanchored_string_pair(
            operand_role(&dividend_type),
            operand_role(divisor_type),
            ansi_mode,
        ) || rejects_as_divide_divisor(&dividend_type)
            || rejects_as_divide_divisor(divisor_type))
    {
        return Err(arithmetic_operand_error("%", &dividend_type, divisor_type));
    }
    // Plan-time check for literal zero divisors.
    if is_zero_literal(&divisor) {
        if function_context.plan_config.ansi_mode {
            return Err(PlanError::ArrowError(ArrowError::ArithmeticOverflow(
                "Remainder by zero".to_string(),
            )));
        } else {
            return Ok(Expr::Literal(ScalarValue::Null, None));
        }
    }

    // Apply runtime zero-divisor guard to the divisor before building the modulo expression.
    let effective_divisor_type = divisor_type.unwrap_or(DataType::Int32);
    let divisor = make_safe_divisor(
        divisor,
        &effective_divisor_type,
        ansi_mode,
        "Remainder by zero",
    );

    Ok(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(dividend),
        op: Operator::Modulo,
        right: Box::new(divisor),
    }))
}

fn spark_abs(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkAbs::new(ansi_mode));
    Ok(udf.call(input.arguments))
}

fn spark_bin(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkBin::new(ansi_mode));
    Ok(udf.call(input.arguments))
}

fn spark_pmod(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkPmod::new(ansi_mode));
    Ok(udf.call(input.arguments))
}

/// Negate a numeric literal at planning time so a constant operand stays a
/// literal (some functions, e.g. `ceil`/`floor` target scale, require a literal
/// argument and run before the optimizer would fold a `SparkNegative` call).
/// Returns `None` when the value is not a foldable numeric literal or the
/// negation overflows (e.g. `-INT_MIN`), leaving such cases to the runtime UDF.
fn negate_literal(arg: &Expr) -> Option<Expr> {
    let Expr::Literal(value, _) = arg else {
        return None;
    };
    let negated = match value {
        ScalarValue::Int8(Some(v)) => ScalarValue::Int8(Some(v.checked_neg()?)),
        ScalarValue::Int16(Some(v)) => ScalarValue::Int16(Some(v.checked_neg()?)),
        ScalarValue::Int32(Some(v)) => ScalarValue::Int32(Some(v.checked_neg()?)),
        ScalarValue::Int64(Some(v)) => ScalarValue::Int64(Some(v.checked_neg()?)),
        ScalarValue::Float32(Some(v)) => ScalarValue::Float32(Some(-v)),
        ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(-v)),
        _ => return None,
    };
    Some(lit(negated))
}

/// Spark unary minus / `negative(x)`. Duration negation goes through
/// `NegateDuration`; everything else uses `SparkNegative`, which honors the ANSI
/// overflow semantics with `ansi_mode` baked at planning time.
fn spark_unary_negate(arg: Expr, ansi_mode: bool, schema: &DFSchemaRef) -> Expr {
    match arg.get_type(schema) {
        // DataFusion's `Negative` doesn't support Duration types, so route those
        // to the dedicated UDF.
        Ok(DataType::Duration(_)) => ScalarUDF::from(NegateDuration::new()).call(vec![arg]),
        // Spark's unary minus coerces strings to DOUBLE before negating. The
        // cast honors ANSI mode: an invalid string is NULL under ANSI off and
        // errors under ANSI on. (Without this, the `SparkNegative` signature
        // would coerce the string to an interval instead.)
        Ok(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            let casted = if ansi_mode {
                cast(arg, DataType::Float64)
            } else {
                try_cast(arg, DataType::Float64)
            };
            ScalarUDF::from(SparkNegative::new(ansi_mode)).call(vec![casted])
        }
        // Floating-point negation never overflows and is identical in both ANSI
        // modes, so use the native (vectorized, foldable) operator.
        Ok(DataType::Float16 | DataType::Float32 | DataType::Float64) => {
            Expr::Negative(Box::new(arg))
        }
        // A negated numeric literal folds to a literal so constant-arg functions
        // (e.g. `ceil`/`floor` target scale) still see a constant; overflow
        // (`-INT_MIN`) can't fold and falls through to the runtime UDF.
        _ => match negate_literal(&arg) {
            Some(folded) => folded,
            None => ScalarUDF::from(SparkNegative::new(ansi_mode)).call(vec![arg]),
        },
    }
}

fn spark_negative(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    Ok(spark_unary_negate(
        arg,
        function_context.plan_config.ansi_mode,
        function_context.schema,
    ))
}

pub(super) fn list_built_in_math_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("%", F::custom(spark_modulo)),
        ("*", F::custom(spark_multiply)),
        ("+", F::custom(spark_plus)),
        ("-", F::custom(spark_minus)),
        ("/", F::custom(spark_divide)),
        ("abs", F::custom(spark_abs)),
        ("acos", F::unary(double(expr_fn::acos))),
        ("acosh", F::unary(double(expr_fn::acosh))),
        ("asin", F::unary(double(expr_fn::asin))),
        ("asinh", F::unary(double(expr_fn::asinh))),
        ("atan", F::unary(double(expr_fn::atan))),
        ("atan2", F::binary(double2(expr_fn::atan2))),
        ("atanh", F::unary(double(expr_fn::atanh))),
        ("bin", F::custom(spark_bin)),
        ("bround", F::udf(SparkBRound::new())),
        ("cbrt", F::unary(double(expr_fn::cbrt))),
        ("ceil", F::custom(|arg| ceil_floor(arg, "ceil"))),
        ("ceiling", F::custom(|arg| ceil_floor(arg, "ceil"))),
        ("conv", F::udf(SparkConv::new())),
        ("cos", F::unary(double(expr_fn::cos))),
        ("cosh", F::unary(double(expr_fn::cosh))),
        ("cot", F::unary(double(expr_fn::cot))),
        ("csc", F::unary(double(|arg| lit(1.0) / expr_fn::sin(arg)))),
        ("degrees", F::unary(double(expr_fn::degrees))),
        ("div", F::custom(spark_div)),
        ("e", F::nullary(eulers_constant)),
        ("exp", F::unary(double(expr_fn::exp))),
        ("expm1", F::unary(math_fn::expm1)),
        ("factorial", F::unary(expr_fn::factorial)),
        ("floor", F::custom(|arg| ceil_floor(arg, "floor"))),
        ("greatest", F::var_arg(expr_fn::greatest)),
        ("hex", F::unary(math_fn::hex)),
        ("hypot", F::binary(hypot)),
        ("least", F::var_arg(expr_fn::least)),
        ("ln", F::unary(double(ln))),
        ("log", F::binary(double2(log))),
        ("log10", F::unary(double(log10))),
        ("log1p", F::unary(double(log1p))),
        ("log2", F::unary(double(log2))),
        ("mod", F::custom(spark_modulo)),
        ("negative", F::custom(spark_negative)),
        ("pi", F::nullary(expr_fn::pi)),
        ("pmod", F::custom(spark_pmod)),
        ("positive", F::unary(positive)),
        ("pow", F::binary(power)),
        ("power", F::binary(power)),
        ("radians", F::unary(double(expr_fn::radians))),
        ("rand", F::udf(Random::new())),
        ("random_poisson", F::udf(RandPoisson::new())),
        ("randn", F::udf(Randn::new())),
        ("random", F::udf(Random::new())),
        ("rint", F::unary(rint)),
        ("round", F::var_arg(expr_fn::round)),
        ("sec", F::unary(double(|arg| lit(1.0) / expr_fn::cos(arg)))),
        ("sign", F::udf(SparkSignum::new())),
        ("signum", F::udf(SparkSignum::new())),
        ("sin", F::unary(double(expr_fn::sin))),
        ("sinh", F::unary(double(expr_fn::sinh))),
        ("sqrt", F::unary(spark_sqrt)),
        ("tan", F::unary(double(expr_fn::tan))),
        ("tanh", F::unary(double(expr_fn::tanh))),
        ("try_add", F::udf(SparkTryAdd::new())),
        ("try_divide", F::udf(SparkTryDiv::new())),
        ("try_multiply", F::udf(SparkTryMult::new())),
        ("try_mod", F::udf(SparkTryMod::new())),
        ("try_subtract", F::udf(SparkTrySubtract::new())),
        ("unhex", F::udf(SparkUnHex::new())),
        ("uniform", F::udf(SparkUniform::new())),
        ("width_bucket", F::quaternary(math_fn::width_bucket)),
    ]
}

/// The Spark arithmetic operand class of a type. `+`, `-` and `*` decide accept/reject by these
/// classes, so the plan-time guards below are written against them. The per-operator accept sets
/// were validated cell-by-cell against Spark JVM 4.2.0 when this table was built, and both sides
/// are asserted in the suite: `math/arithmetic_operand_rejection.feature` pins every pair Spark
/// rejects, and `math/arithmetic_operand_resolution.feature` pins every pair both engines
/// resolve — so narrowing a guard too far turns a row of the latter red. The latter asserts
/// resolution only, never the result type: that is the coercion contract, not this one.
/// `Unsupported` is a type Spark never accepts in arithmetic that would otherwise compute a
/// garbage value: boolean and every binary width, including the fixed-size one a Parquet
/// FIXED_LEN_BYTE_ARRAY produces. `Other` is any type outside the validated matrix (dictionary,
/// run-end-encoded, union -- struct, list, map and time have their own roles): the guards leave
/// those to DataFusion rather than hard-reject a pair whose behavior was never measured.
#[derive(PartialEq, Eq, Clone, Copy)]
enum OperandRole {
    Numeric,
    Str,
    UntypedNull,
    Date,
    Timestamp,
    Time,
    IntervalDt,
    IntervalCalendar,
    IntervalYm,
    Unsupported,
    Other,
}

fn operand_role(data_type: &DataType) -> OperandRole {
    use OperandRole::*;
    if data_type.is_numeric() {
        return Numeric;
    }
    if data_type.is_string() {
        return Str;
    }
    match data_type {
        DataType::Null => UntypedNull,
        DataType::Date32 | DataType::Date64 => Date,
        DataType::Timestamp(_, _) => Timestamp,
        // Spark's TIME (`spark.sql.timeType.enabled`) is an `AnyTimeType`, not a numeric, so
        // `Add`/`Subtract` (`inputType = NumericAndInterval`, `arithmetic.scala:417,508`) and
        // `Multiply` (`inputType = NumericType`, `:591`) reject every pair the datetime resolver
        // does not rewrite -- an interval survives `*` only via that rewrite, never via
        // `inputType`. Classified rather than left to the `Other` role,
        // which defers to DataFusion and lets `date + time` resolve to a TIMESTAMP_NTZ that
        // Spark rejects at analysis.
        DataType::Time32(_) | DataType::Time64(_) => Time,
        DataType::Interval(IntervalUnit::YearMonth) => IntervalYm,
        // Arrow has two spellings of Spark's day-time interval: `Duration`, which the resolver
        // produces (`resolver/data_type.rs:145`, chosen for microsecond precision), and
        // `Interval(DayTime)`, which Sail maps to Spark's `DayTimeInterval` on the way out
        // (`data_type_arrow.rs:200`) and renders as `interval day to second`
        // (`formatter.rs:81`). Both must share the role, or the guard would judge one of Spark's
        // day-time intervals by the calendar rules.
        DataType::Duration(_) | DataType::Interval(IntervalUnit::DayTime) => IntervalDt,
        // Spark's legacy CalendarInterval (`make_interval`), which Arrow stores as
        // `Interval(MonthDayNano)`. It is NOT an `AnsiIntervalType`: Spark pairs it with a date,
        // timestamp, string or another calendar interval, and rejects it against a day-time
        // interval or a TIME, so it cannot share the day-time role.
        DataType::Interval(_) => IntervalCalendar,
        // Spark's Parquet reader maps an unannotated FIXED_LEN_BYTE_ARRAY to `BinaryType`
        // (`ParquetSchemaConverter.scala`), so a fixed-size binary reaches arithmetic as a
        // plain BINARY and Spark rejects it like the other widths.
        DataType::Boolean
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Unsupported,
        // Containers (VARIANT included — Sail stores it as a struct). Spark rejects them for
        // every arithmetic operator against every operand: measured across all 2080 cells of the
        // cartesian product, it accepts none. They are `Unsupported` rather than `Other` so the
        // additive and multiplicative guards reject at plan time and name the Spark type, as `/`
        // and `%` already do, instead of deferring to DataFusion — which multiplies a list by a
        // duration and leaks `List(non-null Int32)` into the message.
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => Unsupported,
        _ => Other,
    }
}

/// The verdict every `+`/`-`/`*` guard shares before its per-operator rules: an `Unsupported`
/// operand (boolean, binary) is always rejected, and an `Other` operand (a type outside the
/// validated matrix — dictionary, run-end-encoded, union) is deferred to
/// DataFusion rather than hard-rejected.
/// `None` means neither applies, so the caller runs its own accept/reject logic.
fn framing_verdict(a: OperandRole, b: OperandRole) -> Option<bool> {
    use OperandRole::*;
    if a == Unsupported || b == Unsupported {
        return Some(true);
    }
    if a == Other || b == Other {
        return Some(false);
    }
    None
}

/// A string paired with another string or an untyped `NULL`, with no numeric operand to anchor the
/// implicit cast. Spark accepts such a pair only under ANSI off (both coerce to DOUBLE); under ANSI
/// on it stays a string arithmetic and fails analysis. `NULL` paired with `NULL` is not included.
fn unanchored_string_pair(a: OperandRole, b: OperandRole) -> bool {
    use OperandRole::*;
    matches!((a, b), (Str, Str) | (Str, UntypedNull) | (UntypedNull, Str))
}

/// [`unanchored_string_pair`] as Spark rejects it: only under ANSI on.
fn rejects_unanchored_string_pair(a: OperandRole, b: OperandRole, ansi_mode: bool) -> bool {
    ansi_mode && unanchored_string_pair(a, b)
}

/// The numeric offset Spark accepts for a `DATE` in `+`/`-`: `DateAdd`/`DateSub` take an
/// `INT` (`IntegerType | ShortType | ByteType`), so only integrals that fit losslessly in an
/// `INT` qualify. A `BIGINT`, `FLOAT`, `DOUBLE` or `DECIMAL` offset is rejected at analysis
/// (Sail would otherwise silently truncate it). Spark has no unsigned types, but Arrow-native
/// sources can, so `UInt8`/`UInt16` (both within `INT` range) are accepted too.
fn is_date_offset_numeric(data_type: &DataType) -> bool {
    // `DateAdd`/`DateSub` take `TypeCollection(IntegerType, ShortType, ByteType)`
    // (`datetimeExpressions.scala:331,371`) and are `ExpectsInputTypes`, not
    // `ImplicitCastInputTypes`, so Spark rejects a BIGINT offset. Sail accepts one anyway,
    // because Sail types several expressions WIDER than Spark does and refusing BIGINT here
    // would refuse queries Spark answers:
    //   * `datediff` / `date_diff` -- Spark `IntegerType` (`datetimeExpressions.scala:2522`)
    //   * `date - date`            -- Spark `DayTimeIntervalType(DAY)` (`:3617`)
    //   * `regexp_count`           -- Spark `IntegerType`
    // are all BIGINT in Sail. `date_offset_view` only rescues the syntactic `date - date`; once
    // the value crosses a projection boundary it is a bare BIGINT column.
    // `UInt32` follows the same rule: Sail's Arrow->Spark mapping reports it as INT
    // (`data_type_arrow.rs`), so refusing it would contradict the schema Sail advertises.
    // `UInt64` stays out: Sail promotes the sum to `Decimal128(21,0)`, which does not cast
    // back to a date, and Spark rejects it too (its reader widens UINT_64 to DECIMAL(20,0)).
    // TODO: give `datediff`, `date_diff` and `date - date` Spark's result types, then narrow
    // this back to `DateAdd`'s accept set (`Int8 | Int16 | Int32 | UInt8 | UInt16`).
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
    )
}

/// The type the date-offset rules should judge an operand by. Spark types `datediff`/`date_diff`
/// as INT (`datetimeExpressions.scala:2522`) and `date - date` as `INTERVAL DAY`
/// (`datetimeExpressions.scala:3616-3618`), and a DATE takes either. Sail computes all three as a
/// BIGINT difference of the two dates, which the INT-width check would reject.
///
/// TODO: give `datediff`/`date_diff` and `date - date` Spark's result types, then drop this.
fn date_offset_view(expr: &Expr, data_type: &DataType, schema: &DFSchemaRef) -> DataType {
    let is_date = |expr: &Expr| {
        let expr = match expr {
            Expr::Cast(cast) => cast.expr.as_ref(),
            other => other,
        };
        matches!(
            expr.get_type(schema),
            Ok(DataType::Date32 | DataType::Date64)
        )
    };
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Minus,
            right,
        }) if is_date(left) && is_date(right) => DataType::Int32,
        _ => data_type.clone(),
    }
}

/// Whether Spark rejects this operand pair for `*` (`Multiply`) at analysis. `*` accepts only
/// numeric×numeric and interval×numeric (either order, with string→numeric coercion); a
/// datetime, boolean, binary, or interval×interval pair is rejected. Left to DataFusion, several
/// of these reinterpret an operand as raw integer and compute a meaningless product.
fn rejects_multiply(left: &DataType, right: &DataType, ansi_mode: bool) -> bool {
    use OperandRole::*;
    let (a, b) = (operand_role(left), operand_role(right));
    if let Some(verdict) = framing_verdict(a, b) {
        return verdict;
    }
    if matches!(a, Date | Timestamp | Time) || matches!(b, Date | Timestamp | Time) {
        return true;
    }
    let is_interval = |r: OperandRole| matches!(r, IntervalDt | IntervalYm | IntervalCalendar);
    if is_interval(a) && is_interval(b) {
        return true;
    }
    // Exactly one operand is an interval. `framing_verdict` already removed `Unsupported` and
    // `Other`, the datetime roles were rejected above, and the both-interval case was handled, so
    // the peer can only be `Numeric`, `Str` or `UntypedNull` -- every one of which Spark accepts,
    // because the resolver rewrites `interval * number` to `Multiply*Interval`
    // (`BinaryArithmeticWithDatetimeResolver.scala:149-154`). Nothing left to reject.
    if is_interval(a) || is_interval(b) {
        return false;
    }
    rejects_unanchored_string_pair(a, b, ansi_mode)
}

/// Whether Spark rejects this operand pair for `+` (`Add`) at analysis. `Add` is commutative, so
/// the accept rule is symmetric: numeric×numeric, date/timestamp ± interval, date + INT offset,
/// same-class interval±interval, and the datetime/string forms Spark's datetime resolver allows.
fn rejects_add(left: &DataType, right: &DataType, ansi_mode: bool) -> bool {
    use OperandRole::*;
    let (a, b) = (operand_role(left), operand_role(right));
    if let Some(verdict) = framing_verdict(a, b) {
        return verdict;
    }
    let numlike = |r: OperandRole| matches!(r, Numeric | Str | UntypedNull);
    if numlike(a) && numlike(b) {
        return rejects_unanchored_string_pair(a, b, ansi_mode);
    }
    // A DATE takes an interval, an untyped NULL, or an INT-width numeric offset (`DateAdd`, whose
    // `days` input is `IntegerType | ShortType | ByteType`); a wider integral, a string, another
    // date or a timestamp is rejected.
    if a == Date || b == Date {
        let (other, other_type) = if a == Date { (b, right) } else { (a, left) };
        return match other {
            UntypedNull | IntervalDt | IntervalYm | IntervalCalendar => false,
            Numeric => !is_date_offset_numeric(other_type),
            _ => true,
        };
    }
    // A TIMESTAMP takes only an interval or an untyped NULL (no numeric offset).
    if a == Timestamp || b == Timestamp {
        let other = if a == Timestamp { b } else { a };
        return !matches!(
            other,
            IntervalDt | IntervalYm | UntypedNull | IntervalCalendar
        );
    }
    // A TIME takes only a day-time interval (`TimeAddInterval`, whose `inputTypes` are
    // `(AnyTimeType, DayTimeIntervalType)`) or an untyped NULL, which the datetime resolver casts
    // to the default day-time interval (`TimeType` is a `DatetimeType`, so it reaches that arm).
    // Another TIME, a year-month interval, a numeric or a string leaves `Add` with its own
    // `inputType` (`NumericAndInterval`), which rejects them.
    if a == Time || b == Time {
        let other = if a == Time { b } else { a };
        return !matches!(other, IntervalDt | UntypedNull);
    }
    // Both operands are intervals (date/timestamp handled above): same class only, except a
    // day-time interval also accepts a string or untyped NULL peer (Spark's day-time resolver).
    match (a, b) {
        (IntervalDt, IntervalDt)
        | (IntervalYm, IntervalYm)
        | (IntervalCalendar, IntervalCalendar) => false,
        (
            IntervalDt | IntervalYm | IntervalCalendar,
            IntervalDt | IntervalYm | IntervalCalendar,
        ) => true,
        _ => {
            let (interval, other) = if matches!(a, IntervalDt | IntervalYm | IntervalCalendar) {
                (a, b)
            } else {
                (b, a)
            };
            match other {
                UntypedNull => false,
                Str => !matches!(interval, IntervalDt | IntervalCalendar),
                _ => true,
            }
        }
    }
}

/// Whether Spark rejects this operand pair for `-` (`Subtract`) at analysis. Unlike `+`,
/// subtraction is NOT commutative — `date - date` yields an interval but `numeric - date` never
/// resolves, `str - date` is accepted while `date - str` needs ANSI — so the accept set is an
/// ordered table validated cell-by-cell against Spark 4.2.0. Left to DataFusion, the rejected
/// pairs reinterpret an operand as raw integer and compute a meaningless difference.
fn rejects_subtract(left: &DataType, right: &DataType, ansi_mode: bool) -> bool {
    use OperandRole::*;
    let (a, b) = (operand_role(left), operand_role(right));
    if let Some(verdict) = framing_verdict(a, b) {
        return verdict;
    }
    // `date - <INT offset>` (`DateSub`); `<numeric> - date` never resolves and falls through.
    if a == Date && b == Numeric {
        return !is_date_offset_numeric(right);
    }
    let accepted = matches!(
        (a, b),
        (Date, Date)
            | (Date, IntervalDt)
            | (Date, IntervalYm)
            | (Date, Timestamp)
            | (Date, UntypedNull)
            | (IntervalDt, IntervalDt)
            | (IntervalDt, UntypedNull)
            | (IntervalYm, IntervalYm)
            | (IntervalYm, UntypedNull)
            | (Numeric, Numeric)
            | (Numeric, Str)
            | (Numeric, UntypedNull)
            | (Str, Date)
            | (Str, IntervalDt)
            | (Str, Numeric)
            | (Timestamp, Date)
            | (Timestamp, IntervalDt)
            | (Timestamp, IntervalYm)
            | (Timestamp, Timestamp)
            | (Timestamp, UntypedNull)
            | (UntypedNull, Date)
            | (UntypedNull, IntervalDt)
            | (UntypedNull, IntervalYm)
            | (UntypedNull, Numeric)
            | (UntypedNull, Timestamp)
            | (UntypedNull, UntypedNull)
            // `TimeAddInterval` with a negated interval, `SubtractTimes` (`interval hour to
            // second`), and the untyped-NULL arms of the datetime resolver. `interval - time` is
            // absent on purpose: subtraction is not commutative and Spark has no such arm.
            | (Time, IntervalDt)
            | (Time, Time)
            // The calendar interval mirrors the day-time pairs except against a TIME, which
            // `TimeAddInterval` (`(AnyTimeType, DayTimeIntervalType)`) does not accept.
            | (Date, IntervalCalendar)
            | (IntervalCalendar, IntervalCalendar)
            | (IntervalCalendar, UntypedNull)
            | (Str, IntervalCalendar)
            | (Timestamp, IntervalCalendar)
            | (UntypedNull, IntervalCalendar)
            | (Time, UntypedNull)
            | (UntypedNull, Time)
    ) || (!ansi_mode && unanchored_string_pair(a, b))
        || (ansi_mode
            && matches!(
                (a, b),
                (Date, Str) | (Str, Timestamp) | (Timestamp, Str) | (Str, Time) | (Time, Str)
            ));
    !accepted
}

/// A `/` dividend Spark rejects: not numeric and not an interval it could scale
/// (booleans, dates, times, timestamps and binary are reinterpreted as raw integers by
/// DataFusion, producing a meaningless quotient — or, for `time`, an unsupported-kernel
/// error at execution where Spark rejects at analysis).
fn rejects_as_divide_dividend(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
            // Container types (and VARIANT, which is stored as a struct). Spark rejects them
            // at ANALYSIS with DATATYPE_MISMATCH for every arithmetic operator, while `/`
            // otherwise lets them fall through to a `Float64` cast that fails in the EXECUTOR
            // — a runtime error where Spark has an analysis one, so a never-evaluated row
            // changes the outcome. `Dictionary` and `RunEndEncoded` are deliberately absent:
            // they wrap a value type that may well be numeric.
            | DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
    )
}

/// A `/` divisor Spark rejects: everything a dividend rejects, plus intervals/durations —
/// Spark has no "number / interval", so DataFusion dividing by the interval's raw nanos is
/// a silent wrong value.
fn rejects_as_divide_divisor(data_type: &DataType) -> bool {
    rejects_as_divide_dividend(data_type)
        || matches!(data_type, DataType::Interval(_) | DataType::Duration(_))
}

/// Spark rejects a UDT operand for every arithmetic operator: a UDT is none of the input types
/// the five operators accept (`Expression.scala:840-857`), whatever it is stored as. Sail keeps
/// UDT identity in the field metadata rather than in the `DataType`, so this is the one operand
/// check [`operand_role`] cannot make -- it would judge the storage type underneath instead.
fn rejects_udt_operand(
    op: &str,
    left: &Expr,
    right: &Expr,
    schema: &DFSchemaRef,
) -> Option<PlanError> {
    let (left_udt, right_udt) = (
        operand_udt_field(left, schema),
        operand_udt_field(right, schema),
    );
    if left_udt.is_none() && right_udt.is_none() {
        return None;
    }
    let name = |expr: &Expr, udt: Option<FieldRef>| {
        udt.or_else(|| expr.to_field(schema).ok().map(|(_, field)| field))
            .map_or_else(
                || "UNKNOWN".to_string(),
                |field| spark_field_type_name(&field),
            )
    };
    Some(PlanError::analysis(format!(
        "cannot resolve arithmetic '{op}' with operand types {} and {}",
        name(left, left_udt),
        name(right, right_udt)
    )))
}

/// The UDT field an arithmetic operand evaluates to. Only a column (or a struct field of one)
/// carries the UDT metadata on its own field; the expressions that return one of their inputs
/// unchanged -- `coalesce`/`nvl`, `nvl2`, `nullif`, `CASE`/`if`, and an array or map element
/// access -- build their result field without it, so they are looked through to the value they
/// return.
fn operand_udt_field(expr: &Expr, schema: &DFSchemaRef) -> Option<FieldRef> {
    // A cast yields its target type, never a UDT. DataFusion copies the source field's metadata
    // onto the cast's output field, so without this stop `CAST(udt AS STRING)` would still read
    // as a UDT and a query Spark resolves would be rejected.
    if matches!(expr, Expr::Cast(_) | Expr::TryCast(_)) {
        return None;
    }
    if let Ok((_, field)) = expr.to_field(schema)
        && is_spark_udt_field(&field)
    {
        return Some(field);
    }
    match expr {
        Expr::Alias(alias) => operand_udt_field(&alias.expr, schema),
        Expr::Case(case) => case
            .when_then_expr
            .iter()
            .map(|(_, then)| then.as_ref())
            .chain(case.else_expr.as_deref())
            .find_map(|branch| operand_udt_field(branch, schema)),
        Expr::ScalarFunction(function) => match function.func.name() {
            "coalesce" | "nvl" => function
                .args
                .iter()
                .find_map(|arg| operand_udt_field(arg, schema)),
            // `nvl2(x, y, z)` returns `y` or `z`.
            "nvl2" => function
                .args
                .iter()
                .skip(1)
                .find_map(|arg| operand_udt_field(arg, schema)),
            // `nullif(a, b)` returns `a` or NULL.
            "nullif" => function
                .args
                .first()
                .and_then(|arg| operand_udt_field(arg, schema)),
            "array_element" => function
                .args
                .first()
                .and_then(|collection| collection_element_udt_field(collection, schema)),
            _ => None,
        },
        _ => None,
    }
}

/// The UDT element field of an array, or of the value list `map_extract` pulls out of a map. The
/// resolver builds both element fields through `resolve_field`, so they keep the UDT metadata.
fn collection_element_udt_field(collection: &Expr, schema: &DFSchemaRef) -> Option<FieldRef> {
    if let Expr::ScalarFunction(function) = collection
        && function.func.name() == "map_extract"
    {
        return match function.args.first()?.get_type(schema).ok()? {
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    Some(Arc::clone(&fields[1])).filter(|field| is_spark_udt_field(field))
                }
                _ => None,
            },
            _ => None,
        };
    }
    match collection.get_type(schema).ok()? {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => Some(field).filter(|field| is_spark_udt_field(field)),
        _ => None,
    }
}

/// The plan-time rejection Spark raises at analysis for an arithmetic operand pair it cannot
/// resolve. The `cannot resolve` substring it shares with Spark's `Cannot resolve …` text is what
/// the `.feature` reject scenarios assert.
///
/// TODO: Spark picks the `DATATYPE_MISMATCH` subclass per expression -- `BINARY_OP_DIFF_TYPES`,
/// `BINARY_OP_WRONG_TYPE` or `UNEXPECTED_INPUT_TYPE` -- always with SQLSTATE `42K09`, plus the
/// rewritten expression text and query context. Emit them once Sail has structured analysis
/// errors; `arithmetic_error_metadata.feature` pins the gap.
fn arithmetic_operand_error(op: &str, left: &DataType, right: &DataType) -> PlanError {
    PlanError::analysis(format!(
        "cannot resolve arithmetic '{op}' with operand types {} and {}",
        spark_type_name(left),
        spark_type_name(right)
    ))
}
