use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array};
use datafusion::arrow::compute::kernels::nullif::nullif;
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::{adjust_offsets_for_slice, list_values};
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, LambdaArgument, LambdaParametersProgress, ValueOrLambda, Volatility,
};

/// Spark-compatible `aggregate`/`reduce` higher-order function.
///
/// Reference: <https://spark.apache.org/docs/latest/api/sql/index.html#aggregate>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayReduce {
    signature: HigherOrderSignature,
}

impl Default for SparkArrayReduce {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayReduce {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::user_defined(Volatility::Immutable),
        }
    }
}

impl HigherOrderUDF for SparkArrayReduce {
    fn name(&self) -> &str {
        "spark_array_reduce"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let (list, zero) = match arg_types {
            [l, z] => (l, z),
            _ => {
                return plan_err!(
                    "{} requires exactly 2 value arguments (array, initial), got {}",
                    self.name(),
                    arg_types.len()
                )
            }
        };
        let coerced_list = match list {
            DataType::List(_) | DataType::LargeList(_) => list.clone(),
            DataType::ListView(f) | DataType::FixedSizeList(f, _) => DataType::List(Arc::clone(f)),
            DataType::LargeListView(f) => DataType::LargeList(Arc::clone(f)),
            _ => {
                return plan_err!(
                    "{} expected a list as first argument, got {}",
                    self.name(),
                    list
                )
            }
        };
        Ok(vec![coerced_list, zero.clone()])
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        // fields: [list_field, zero_field, merge_output_or_none, (optional) finish_output_or_none]
        let has_finish = fields.len() == 4;

        let list_field = match fields.first() {
            Some(ValueOrLambda::Value(f)) => f,
            _ => return plan_err!("{}: first arg must be a value (list)", self.name()),
        };
        let zero_field = match fields.get(1) {
            Some(ValueOrLambda::Value(f)) => f,
            _ => return plan_err!("{}: second arg must be a value (initial)", self.name()),
        };

        let elem_type = match list_field.data_type() {
            DataType::List(f) | DataType::LargeList(f) => f.data_type().clone(),
            _ => {
                return plan_err!(
                    "{}: expected list type, got {}",
                    self.name(),
                    list_field.data_type()
                )
            }
        };

        // acc param uses zero's type; elem param uses array element type (both nullable)
        let acc_field = Arc::new(Field::new("", zero_field.data_type().clone(), true));
        let elem_field = Arc::new(Field::new("", elem_type, true));

        if !has_finish {
            // 3-arg form (reduce): Complete at step 0 — merge params are fully known
            return Ok(LambdaParametersProgress::Complete(vec![vec![
                acc_field, elem_field,
            ]]));
        }

        // 4-arg form (aggregate): finish params depend on merge output type
        let merge_known = match fields.get(2) {
            Some(ValueOrLambda::Value(f)) => Some(Arc::clone(f)),
            Some(ValueOrLambda::Lambda(Some(f))) => Some(Arc::clone(f)),
            _ => None,
        };

        match merge_known {
            Some(merge_out) => {
                let finish_acc = Arc::new(Field::new("", merge_out.data_type().clone(), true));
                Ok(LambdaParametersProgress::Complete(vec![
                    vec![acc_field, elem_field],
                    vec![finish_acc],
                ]))
            }
            None => Ok(LambdaParametersProgress::Partial(vec![
                Some(vec![acc_field, elem_field]),
                None,
            ])),
        }
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<Arc<Field>> {
        let output_type = match args.arg_fields.last() {
            Some(ValueOrLambda::Lambda(f)) => f.data_type().clone(),
            Some(ValueOrLambda::Value(f)) => f.data_type().clone(),
            None => return plan_err!("{}: no arguments provided", self.name()),
        };
        Ok(Arc::new(Field::new("", output_type, true)))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let n_rows = args.number_rows;

        let (list_cv, zero_cv, merge_lambda, finish_opt) = extract_reduce_args(&args.args)?;

        let list_array = list_cv.to_array(n_rows)?;

        if list_array.data_type() == &DataType::Null {
            return Ok(ColumnarValue::Array(
                datafusion::arrow::array::new_null_array(args.return_field.data_type(), n_rows),
            ));
        }

        let flat_values = list_values(&list_array)?;
        let (offsets, null_mask) = extract_list_offsets_and_nulls(&list_array)?;
        let n = offsets.len().saturating_sub(1);
        let lengths: Vec<usize> = (0..n)
            .map(|i| {
                if null_mask[i] {
                    0
                } else {
                    (offsets[i + 1] - offsets[i]) as usize
                }
            })
            .collect();
        let max_len = lengths.iter().max().copied().unwrap_or(0);

        // Initial accumulator: broadcast scalar to N rows if needed
        let mut acc: ArrayRef = zero_cv.to_array(n_rows)?;

        for depth in 0..max_len {
            let active_mask =
                BooleanArray::from((0..n).map(|i| depth < lengths[i]).collect::<Vec<bool>>());

            let elem_indices = Int64Array::from(
                (0..n)
                    .map(|i| {
                        if depth < lengths[i] {
                            Some(offsets[i] as i64 + depth as i64)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<i64>>>(),
            );
            let elements = take(flat_values.as_ref(), &elem_indices, None)?;

            let acc_snap = Arc::clone(&acc);
            let elem_snap = Arc::clone(&elements);

            let new_acc_cv = merge_lambda.evaluate(
                &[&|| Ok(Arc::clone(&acc_snap)), &|| {
                    Ok(Arc::clone(&elem_snap))
                }],
                |captures| Ok(captures.to_vec()),
            )?;
            let new_acc = new_acc_cv.to_array(n_rows)?;

            // Update only active rows; inactive rows keep their previous accumulator
            acc = zip(&active_mask, &new_acc, &acc)?;
        }

        // Apply finish lambda if present (always called, even for NULL acc)
        let result = if let Some(finish) = finish_opt {
            let acc_snap = Arc::clone(&acc);
            let result_cv = finish.evaluate(&[&|| Ok(Arc::clone(&acc_snap))], |captures| {
                Ok(captures.to_vec())
            })?;
            result_cv.to_array(n_rows)?
        } else {
            acc
        };

        // Null-array rows produce NULL output regardless of merge/finish
        if null_mask.iter().any(|&b| b) {
            let null_mask_array = BooleanArray::from(null_mask);
            return Ok(ColumnarValue::Array(nullif(
                result.as_ref(),
                &null_mask_array,
            )?));
        }

        Ok(ColumnarValue::Array(result))
    }
}

fn extract_reduce_args<'a>(
    args: &'a [ValueOrLambda<ColumnarValue, LambdaArgument>],
) -> Result<(
    &'a ColumnarValue,
    &'a ColumnarValue,
    &'a LambdaArgument,
    Option<&'a LambdaArgument>,
)> {
    match args {
        [ValueOrLambda::Value(l), ValueOrLambda::Value(z), ValueOrLambda::Lambda(m)] => {
            Ok((l, z, m, None))
        }
        [ValueOrLambda::Value(l), ValueOrLambda::Value(z), ValueOrLambda::Lambda(m), ValueOrLambda::Lambda(f)] => {
            Ok((l, z, m, Some(f)))
        }
        _ => exec_err!(
            "reduce/aggregate expects (array, initial, merge[, finish]), got {} args with pattern {:?}",
            args.len(),
            args.iter()
                .map(|a| match a {
                    ValueOrLambda::Value(_) => "value",
                    ValueOrLambda::Lambda(_) => "lambda",
                })
                .collect::<Vec<_>>()
        ),
    }
}

fn extract_list_offsets_and_nulls(list_array: &ArrayRef) -> Result<(Vec<i32>, Vec<bool>)> {
    match list_array.data_type() {
        DataType::List(_) => {
            let list = list_array.as_list::<i32>();
            let adjusted = adjust_offsets_for_slice(list);
            let offsets = adjusted.as_ref().to_vec();
            let nulls = (0..list.len()).map(|i| list.is_null(i)).collect();
            Ok((offsets, nulls))
        }
        DataType::LargeList(_) => {
            let list = list_array.as_list::<i64>();
            let adjusted = adjust_offsets_for_slice(list);
            let offsets = adjusted.as_ref().iter().map(|&o| o as i32).collect();
            let nulls = (0..list.len()).map(|i| list.is_null(i)).collect();
            Ok((offsets, nulls))
        }
        other => exec_err!("reduce/aggregate: expected list, got {other}"),
    }
}
