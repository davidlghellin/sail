use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, LargeListArray, ListArray};
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::{adjust_offsets_for_slice, list_values, list_values_row_number};
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, LambdaArgument, LambdaParametersProgress, ValueOrLambda, Volatility,
};

/// Spark-compatible `transform(array, function)` higher-order function.
///
/// Reference: <https://spark.apache.org/docs/latest/api/sql/index.html#transform>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayTransform {
    signature: HigherOrderSignature,
}

impl Default for SparkArrayTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayTransform {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::user_defined(Volatility::Immutable),
        }
    }
}

impl HigherOrderUDF for SparkArrayTransform {
    fn name(&self) -> &str {
        "spark_array_transform"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [list] = arg_types else {
            return plan_err!(
                "{} requires exactly 1 value argument, got {}",
                self.name(),
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
                return plan_err!(
                    "{} expected a list as first argument, got {}",
                    self.name(),
                    list
                );
            }
        };
        Ok(vec![coerced])
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let (list_field, _lambda) = value_lambda_pair(self.name(), fields)?;
        let elem_field = match list_field.data_type() {
            DataType::List(f) | DataType::LargeList(f) => Arc::clone(f),
            _ => return plan_err!("expected list, got {list_field}"),
        };
        // Expose both (element, index) so 1-arg and 2-arg lambdas both work.
        let index_field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            DataType::Int64,
            false,
        ));
        Ok(LambdaParametersProgress::Complete(vec![vec![
            elem_field,
            index_field,
        ]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<Arc<Field>> {
        let (list_field, lambda_field) = value_lambda_pair(self.name(), args.arg_fields)?;
        // Output element type is the lambda return type, not the input element type.
        let output_elem_field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            lambda_field.data_type().clone(),
            lambda_field.is_nullable(),
        ));
        let return_type = match list_field.data_type() {
            DataType::List(_) => DataType::List(output_elem_field),
            DataType::LargeList(_) => DataType::LargeList(output_elem_field),
            other => return plan_err!("expected list, got {other}"),
        };
        Ok(Arc::new(Field::new(
            self.name(),
            return_type,
            list_field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (list, lambda) = value_lambda_pair_args(self.name(), &args.args)?;
        let list_array = list.to_array(args.number_rows)?;

        let flat_values = list_values(&list_array)?;
        let flat_len = flat_values.len();

        let output_elem_field = match args.return_field.data_type() {
            DataType::List(f) | DataType::LargeList(f) => Arc::clone(f),
            other => {
                return exec_err!(
                    "{} expected return_field to be a list, got {}",
                    self.name(),
                    other
                )
            }
        };

        match list_array.data_type() {
            DataType::List(_) => {
                let list = list_array.as_list::<i32>();
                let adjusted = adjust_offsets_for_slice(list);
                let per_sublist_indices =
                    build_per_sublist_indices_i32(adjusted.as_ref(), list.len());
                let indices: ArrayRef = Arc::new(per_sublist_indices);
                let fv = Arc::clone(&flat_values);
                let transformed =
                    eval_transform(lambda, list_array.as_ref(), fv, indices, flat_len)?;
                Ok(ColumnarValue::Array(Arc::new(ListArray::new(
                    output_elem_field,
                    adjusted,
                    transformed,
                    list.nulls().cloned(),
                ))))
            }
            DataType::LargeList(_) => {
                let list = list_array.as_list::<i64>();
                let adjusted = adjust_offsets_for_slice(list);
                let per_sublist_indices =
                    build_per_sublist_indices_i64(adjusted.as_ref(), list.len());
                let indices: ArrayRef = Arc::new(per_sublist_indices);
                let fv = Arc::clone(&flat_values);
                let transformed =
                    eval_transform(lambda, list_array.as_ref(), fv, indices, flat_len)?;
                Ok(ColumnarValue::Array(Arc::new(LargeListArray::new(
                    output_elem_field,
                    adjusted,
                    transformed,
                    list.nulls().cloned(),
                ))))
            }
            other => exec_err!("expected list, got {other}"),
        }
    }
}

fn build_per_sublist_indices_i32(offsets: &[i32], n_rows: usize) -> Int64Array {
    let capacity = offsets.last().copied().unwrap_or(0) as usize;
    let mut indices = Vec::with_capacity(capacity);
    for row_idx in 0..n_rows {
        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        for sub_idx in 0..(end - start) {
            indices.push(sub_idx as i64);
        }
    }
    Int64Array::from(indices)
}

fn build_per_sublist_indices_i64(offsets: &[i64], n_rows: usize) -> Int64Array {
    let capacity = offsets.last().copied().unwrap_or(0) as usize;
    let mut indices = Vec::with_capacity(capacity);
    for row_idx in 0..n_rows {
        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        for sub_idx in 0..(end - start) {
            indices.push(sub_idx as i64);
        }
    }
    Int64Array::from(indices)
}

fn eval_transform(
    lambda: &LambdaArgument,
    list_array: &dyn Array,
    flat_values: ArrayRef,
    indices: ArrayRef,
    flat_len: usize,
) -> Result<ArrayRef> {
    let elem_fn = || Ok(Arc::clone(&flat_values));
    let idx_fn = || Ok(Arc::clone(&indices));
    let result_cv = lambda.evaluate(&[&elem_fn, &idx_fn], |arrays| {
        let row_indices = list_values_row_number(list_array)?;
        Ok(take_arrays(arrays, &row_indices, None)?)
    })?;
    result_cv.into_array(flat_len)
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
