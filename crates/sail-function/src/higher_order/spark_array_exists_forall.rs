use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder};
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::{adjust_offsets_for_slice, list_values, list_values_row_number};
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, LambdaArgument, LambdaParametersProgress, ValueOrLambda, Volatility,
};

/// Spark-compatible `exists(array, predicate)` higher-order function.
///
/// Reference: <https://spark.apache.org/docs/latest/api/sql/index.html#exists>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkExists {
    signature: HigherOrderSignature,
}

impl Default for SparkExists {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkExists {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::user_defined(Volatility::Immutable),
        }
    }
}

/// Spark-compatible `forall(array, predicate)` higher-order function.
///
/// Reference: <https://spark.apache.org/docs/latest/api/sql/index.html#forall>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkForall {
    signature: HigherOrderSignature,
}

impl Default for SparkForall {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkForall {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::user_defined(Volatility::Immutable),
        }
    }
}

impl HigherOrderUDF for SparkExists {
    fn name(&self) -> &str {
        "spark_exists"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_list_type(self.name(), arg_types)
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        lambda_params_1(self.name(), fields)
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<Arc<Field>> {
        let _ = value_lambda_pair(self.name(), args.arg_fields)?;
        Ok(Arc::new(Field::new("", DataType::Boolean, true)))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        invoke_predicate(args, reduce_or, false)
    }
}

impl HigherOrderUDF for SparkForall {
    fn name(&self) -> &str {
        "spark_forall"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_list_type(self.name(), arg_types)
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        lambda_params_1(self.name(), fields)
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<Arc<Field>> {
        let _ = value_lambda_pair(self.name(), args.arg_fields)?;
        Ok(Arc::new(Field::new("", DataType::Boolean, true)))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        invoke_predicate(args, reduce_and, true)
    }
}

fn coerce_list_type(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let [list] = arg_types else {
        return plan_err!(
            "{} requires exactly 1 value argument, got {}",
            name,
            arg_types.len()
        );
    };
    let coerced = match list {
        DataType::List(_) | DataType::LargeList(_) => list.clone(),
        DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
            DataType::List(Arc::clone(field))
        }
        DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
        _ => {
            return plan_err!("{} expected a list as first argument, got {}", name, list);
        }
    };
    Ok(vec![coerced])
}

// exists/forall only support 1-param lambdas — Spark rejects (x, i) -> ... for these HOFs.
fn lambda_params_1(
    name: &str,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> Result<LambdaParametersProgress> {
    let (list_field, _lambda) = value_lambda_pair(name, fields)?;
    let inner = match list_field.data_type() {
        DataType::List(f) | DataType::LargeList(f) => Arc::clone(f),
        _ => return plan_err!("{name} expected list, got {list_field}"),
    };
    // Force nullable=true to match the LambdaVariable produced by
    // replace_synthetic_columns_with_lambda_vars (which always uses nullable=true).
    let elem_field = Arc::new(inner.as_ref().clone().with_nullable(true));
    Ok(LambdaParametersProgress::Complete(vec![vec![elem_field]]))
}

/// Evaluate predicate lambda and reduce per sublist.
/// `empty_result`: value returned for empty sublists (false for exists, true for forall).
fn invoke_predicate(
    args: HigherOrderFunctionArgs,
    reduce: fn(&BooleanArray, usize, usize) -> Option<bool>,
    empty_result: bool,
) -> Result<ColumnarValue> {
    let (list, lambda) = value_lambda_pair_args("predicate HOF", &args.args)?;
    let list_array = list.to_array(args.number_rows)?;
    let flat_values = list_values(&list_array)?;
    let flat_len = flat_values.len();
    let mask = eval_lambda_1param(lambda, list_array.as_ref(), flat_values, flat_len)?;

    match list_array.data_type() {
        DataType::List(_) => {
            let list = list_array.as_list::<i32>();
            let adjusted = adjust_offsets_for_slice(list);
            let offsets: &[i32] = adjusted.as_ref();
            reduce_per_row(
                &mask,
                |i| (offsets[i] as usize, offsets[i + 1] as usize),
                |i| list.is_null(i),
                list.len(),
                reduce,
                empty_result,
            )
        }
        DataType::LargeList(_) => {
            let list = list_array.as_list::<i64>();
            let adjusted = adjust_offsets_for_slice(list);
            let offsets: &[i64] = adjusted.as_ref();
            reduce_per_row(
                &mask,
                |i| (offsets[i] as usize, offsets[i + 1] as usize),
                |i| list.is_null(i),
                list.len(),
                reduce,
                empty_result,
            )
        }
        other => exec_err!("expected list, got {other}"),
    }
}

fn reduce_per_row(
    mask: &BooleanArray,
    offsets: impl Fn(usize) -> (usize, usize),
    is_null: impl Fn(usize) -> bool,
    n_rows: usize,
    reduce: fn(&BooleanArray, usize, usize) -> Option<bool>,
    empty_result: bool,
) -> Result<ColumnarValue> {
    let mut builder = BooleanBuilder::with_capacity(n_rows);
    for row_idx in 0..n_rows {
        if is_null(row_idx) {
            builder.append_null();
        } else {
            let (start, end) = offsets(row_idx);
            if start == end {
                builder.append_value(empty_result);
            } else {
                match reduce(mask, start, end) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn eval_lambda_1param(
    lambda: &LambdaArgument,
    list_array: &dyn Array,
    flat_values: ArrayRef,
    flat_len: usize,
) -> Result<BooleanArray> {
    let elem_fn = || Ok(Arc::clone(&flat_values));
    let mask_cv = lambda.evaluate(&[&elem_fn], |arrays| {
        let row_indices = list_values_row_number(list_array)?;
        Ok(take_arrays(arrays, &row_indices, None)?)
    })?;
    let mask_array = mask_cv.into_array(flat_len)?;
    mask_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "predicate lambda must return Boolean".to_string(),
            )
        })
}

/// OR with 3-valued SQL logic: true if any true; NULL if any null and no true; false otherwise.
fn reduce_or(mask: &BooleanArray, start: usize, end: usize) -> Option<bool> {
    let mut has_null = false;
    for i in start..end {
        if !mask.is_valid(i) {
            has_null = true;
        } else if mask.value(i) {
            return Some(true);
        }
    }
    if has_null {
        None
    } else {
        Some(false)
    }
}

/// AND with 3-valued SQL logic: false if any false; NULL if any null and no false; true otherwise.
fn reduce_and(mask: &BooleanArray, start: usize, end: usize) -> Option<bool> {
    let mut has_null = false;
    for i in start..end {
        if !mask.is_valid(i) {
            has_null = true;
        } else if !mask.value(i) {
            return Some(false);
        }
    }
    if has_null {
        None
    } else {
        Some(true)
    }
}

fn value_lambda_pair<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a L)> {
    let [value, lambda] = args else {
        return plan_err!(
            "{name} expects exactly 2 arguments (value + lambda), got {}",
            args.len()
        );
    };
    match (value, lambda) {
        (ValueOrLambda::Value(v), ValueOrLambda::Lambda(l)) => Ok((v, l)),
        _ => plan_err!("{name} expects a value followed by a lambda"),
    }
}

fn value_lambda_pair_args<'a>(
    name: &str,
    args: &'a [ValueOrLambda<ColumnarValue, LambdaArgument>],
) -> Result<(&'a ColumnarValue, &'a LambdaArgument)> {
    let [value, lambda] = args else {
        return plan_err!(
            "{name} expects exactly 2 arguments (value + lambda), got {}",
            args.len()
        );
    };
    match (value, lambda) {
        (ValueOrLambda::Value(v), ValueOrLambda::Lambda(l)) => Ok((v, l)),
        _ => plan_err!("{name} expects a value followed by a lambda"),
    }
}
