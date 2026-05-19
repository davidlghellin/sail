use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray, UInt32Array};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::{adjust_offsets_for_slice, list_values};
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, LambdaParametersProgress, ValueOrLambda, Volatility,
};

/// Spark-compatible `zip_with(array1, array2, func)` higher-order function.
///
/// Reference: <https://spark.apache.org/docs/latest/api/sql/index.html#zip_with>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkZipWith {
    signature: HigherOrderSignature,
}

impl Default for SparkZipWith {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkZipWith {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![
                    ValueOrLambda::Value(()),
                    ValueOrLambda::Value(()),
                    ValueOrLambda::Lambda(()),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl HigherOrderUDF for SparkZipWith {
    fn name(&self) -> &str {
        "spark_zip_with"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [list1, list2] = arg_types else {
            return plan_err!(
                "{} requires exactly 2 array arguments, got {}",
                self.name(),
                arg_types.len()
            );
        };
        let coerce = |dt: &DataType| -> Result<DataType> {
            match dt {
                DataType::List(_) | DataType::LargeList(_) => Ok(dt.clone()),
                DataType::ListView(f) | DataType::FixedSizeList(f, _) => {
                    Ok(DataType::List(Arc::clone(f)))
                }
                DataType::LargeListView(f) => Ok(DataType::LargeList(Arc::clone(f))),
                _ => plan_err!("{} expected an array, got {}", self.name(), dt),
            }
        };
        Ok(vec![coerce(list1)?, coerce(list2)?])
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let [ValueOrLambda::Value(list1_field), ValueOrLambda::Value(list2_field), ValueOrLambda::Lambda(_)] =
            fields
        else {
            return plan_err!(
                "{} expects 2 array arguments followed by a lambda",
                self.name()
            );
        };

        let elem_field1 = match list1_field.data_type() {
            DataType::List(f) | DataType::LargeList(f) => {
                Arc::new(f.as_ref().clone().with_nullable(true))
            }
            _ => return plan_err!("expected list, got {list1_field}"),
        };

        let elem_field2 = match list2_field.data_type() {
            DataType::List(f) | DataType::LargeList(f) => {
                Arc::new(f.as_ref().clone().with_nullable(true))
            }
            _ => return plan_err!("expected list, got {list2_field}"),
        };

        Ok(LambdaParametersProgress::Complete(vec![vec![
            elem_field1,
            elem_field2,
        ]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<Arc<Field>> {
        let [ValueOrLambda::Value(_), ValueOrLambda::Value(_), ValueOrLambda::Lambda(lambda_field)] =
            args.arg_fields
        else {
            return plan_err!(
                "{} expects 2 values and 1 lambda, got {} args",
                self.name(),
                args.arg_fields.len()
            );
        };
        let inner = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            lambda_field.data_type().clone(),
            true,
        ));
        Ok(Arc::new(Field::new("", DataType::List(inner), true)))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [cv1, cv2, lambda_vl] = &args.args[..] else {
            return exec_err!("{} requires 3 arguments", self.name());
        };
        let (cv1, cv2, lambda) = match (cv1, cv2, lambda_vl) {
            (ValueOrLambda::Value(v1), ValueOrLambda::Value(v2), ValueOrLambda::Lambda(l)) => {
                (v1, v2, l)
            }
            _ => return exec_err!("{} requires 2 value arrays and 1 lambda", self.name()),
        };

        let arr1 = cv1.to_array(args.number_rows)?;
        let arr2 = cv2.to_array(args.number_rows)?;
        let n_rows = arr1.len();

        let (list1_arr, flat1) = as_list_i32(&arr1)?;
        let (list2_arr, flat2) = as_list_i32(&arr2)?;

        let adj1 = adjust_offsets_for_slice(list1_arr);
        let adj2 = adjust_offsets_for_slice(list2_arr);
        let offs1: &[i32] = adj1.as_ref();
        let offs2: &[i32] = adj2.as_ref();

        // Compute output offsets and total flat length.
        let mut out_offsets: Vec<i32> = Vec::with_capacity(n_rows + 1);
        out_offsets.push(0);
        let mut total_out: i32 = 0;
        for row in 0..n_rows {
            let out_len = if list1_arr.is_null(row) || list2_arr.is_null(row) {
                0
            } else {
                let len1 = offs1[row + 1] - offs1[row];
                let len2 = offs2[row + 1] - offs2[row];
                len1.max(len2)
            };
            total_out += out_len;
            out_offsets.push(total_out);
        }
        let total_out = total_out as usize;

        // Build padded take-indices for both arrays.
        // None → NULL (padding position).
        let mut x_indices: Vec<Option<u32>> = Vec::with_capacity(total_out);
        let mut y_indices: Vec<Option<u32>> = Vec::with_capacity(total_out);
        let mut row_map: Vec<u32> = Vec::with_capacity(total_out);

        for row in 0..n_rows {
            if list1_arr.is_null(row) || list2_arr.is_null(row) {
                continue;
            }
            let start1 = offs1[row] as usize;
            let len1 = (offs1[row + 1] - offs1[row]) as usize;
            let start2 = offs2[row] as usize;
            let len2 = (offs2[row + 1] - offs2[row]) as usize;
            let out_len = len1.max(len2);

            for j in 0..out_len {
                x_indices.push(if j < len1 {
                    Some((start1 + j) as u32)
                } else {
                    None
                });
                y_indices.push(if j < len2 {
                    Some((start2 + j) as u32)
                } else {
                    None
                });
                row_map.push(row as u32);
            }
        }

        // Apply take to build padded flat arrays.
        let x_flat: ArrayRef = if flat1.is_empty() && x_indices.iter().all(|i| i.is_none()) {
            // Both arrays empty / fully null — produce a typed null array
            datafusion::arrow::array::new_null_array(flat1.data_type(), total_out)
        } else {
            datafusion::arrow::compute::take(flat1.as_ref(), &UInt32Array::from(x_indices), None)?
        };
        let y_flat: ArrayRef = if flat2.is_empty() && y_indices.iter().all(|i| i.is_none()) {
            datafusion::arrow::array::new_null_array(flat2.data_type(), total_out)
        } else {
            datafusion::arrow::compute::take(flat2.as_ref(), &UInt32Array::from(y_indices), None)?
        };

        let row_indices_arr: ArrayRef = Arc::new(UInt32Array::from(row_map));

        // Evaluate the lambda body over the flat zipped elements.
        let x_fn = || Ok(Arc::clone(&x_flat));
        let y_fn = || Ok(Arc::clone(&y_flat));
        let result_cv = lambda.evaluate(&[&x_fn, &y_fn], |outer_arrays| {
            Ok(take_arrays(outer_arrays, &row_indices_arr, None)?)
        })?;
        let result_flat = result_cv.into_array(total_out)?;

        // Build null buffer — rows where either input was NULL → output is NULL.
        let null_buf = build_null_buf(n_rows, |i| list1_arr.is_null(i) || list2_arr.is_null(i));

        let inner_field = match args.return_field.data_type() {
            DataType::List(f) => Arc::clone(f),
            other => return exec_err!("{} expected List return type, got {}", self.name(), other),
        };

        let result = ListArray::new(
            inner_field,
            OffsetBuffer::new(out_offsets.into()),
            result_flat,
            null_buf,
        );
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

fn as_list_i32(arr: &ArrayRef) -> Result<(&ListArray, ArrayRef)> {
    match arr.data_type() {
        DataType::List(_) => {
            let list = arr.as_list::<i32>();
            let flat = list_values(arr)?;
            Ok((list, flat))
        }
        other => exec_err!("zip_with: expected List, got {}", other),
    }
}

fn build_null_buf(n_rows: usize, is_null: impl Fn(usize) -> bool) -> Option<NullBuffer> {
    let mut any_null = false;
    let bits: Vec<bool> = (0..n_rows)
        .map(|i| {
            let null = is_null(i);
            if null {
                any_null = true;
            }
            !null
        })
        .collect();
    if any_null {
        Some(NullBuffer::from(bits))
    } else {
        None
    }
}
