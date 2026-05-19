use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, LargeListArray, ListArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::prelude::SessionContext;
use datafusion_common::{exec_err, DFSchema, Result};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use once_cell::sync::OnceCell;

/// Build the Arrow schema for the flattened batch (element + optional index + outer cols).
/// Used both internally and by `transform_lambda` in sail-plan when pre-compiling the PhysicalExpr.
pub fn build_batch_schema(
    element_column_name: &str,
    element_type: &DataType,
    index_column_name: Option<&str>,
    outer_columns: &[(String, DataType)],
) -> Arc<Schema> {
    let mut fields = vec![Field::new(element_column_name, element_type.clone(), true)];
    if let Some(idx_col) = index_column_name {
        fields.push(Field::new(idx_col, DataType::Int32, false));
    }
    for (col_name, col_type) in outer_columns {
        fields.push(Field::new(col_name, col_type.clone(), true));
    }
    Arc::new(Schema::new(fields))
}

/// SparkArrayTransformExpr transforms array elements using arbitrary DataFusion expressions.
///
/// The logical `Expr` is stored for codec serialization. The `PhysicalExpr` is compiled
/// once at planning time (via `with_precompiled`) or lazily on first invocation.
///
/// Unlike `SparkArrayFilterExpr`, the lambda may return any type — the output array element
/// type is determined at planning time from the lambda return type.
#[derive(Debug)]
pub struct SparkArrayTransformExpr {
    signature: Signature,
    /// Logical expression — kept for codec serialization/deserialization.
    logical_expr: Expr,
    /// Compiled physical expression.
    compiled: OnceCell<Arc<dyn PhysicalExpr>>,
    /// Data type of input array elements.
    element_type: DataType,
    /// Return type of the lambda body (= output element type).
    return_element_type: DataType,
    /// Column name used in the expression for the lambda element variable.
    column_name: String,
    /// Optional column name for the index variable (two-argument lambdas).
    index_column_name: Option<String>,
    /// External columns referenced in the lambda expression.
    outer_columns: Vec<(String, DataType)>,
}

impl SparkArrayTransformExpr {
    pub fn new(
        logical_expr: Expr,
        element_type: DataType,
        return_element_type: DataType,
        column_name: String,
        index_column_name: Option<String>,
        outer_columns: Vec<(String, DataType)>,
    ) -> Self {
        let num_args = 1 + outer_columns.len();
        Self {
            signature: Signature::any(num_args, Volatility::Immutable),
            logical_expr,
            compiled: OnceCell::new(),
            element_type,
            return_element_type,
            column_name,
            index_column_name,
            outer_columns,
        }
    }

    pub fn with_precompiled(
        logical_expr: Expr,
        physical_expr: Arc<dyn PhysicalExpr>,
        element_type: DataType,
        return_element_type: DataType,
        column_name: String,
        index_column_name: Option<String>,
        outer_columns: Vec<(String, DataType)>,
    ) -> Self {
        let num_args = 1 + outer_columns.len();
        let compiled = OnceCell::new();
        let _ = compiled.set(physical_expr);
        Self {
            signature: Signature::any(num_args, Volatility::Immutable),
            logical_expr,
            compiled,
            element_type,
            return_element_type,
            column_name,
            index_column_name,
            outer_columns,
        }
    }

    pub fn logical_expr(&self) -> &Expr {
        &self.logical_expr
    }

    pub fn element_type(&self) -> &DataType {
        &self.element_type
    }

    pub fn return_element_type(&self) -> &DataType {
        &self.return_element_type
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn index_column_name(&self) -> Option<&str> {
        self.index_column_name.as_deref()
    }

    pub fn outer_columns(&self) -> &[(String, DataType)] {
        &self.outer_columns
    }

    fn batch_schema(&self) -> Arc<Schema> {
        build_batch_schema(
            &self.column_name,
            &self.element_type,
            self.index_column_name.as_deref(),
            &self.outer_columns,
        )
    }

    fn physical_expr(&self) -> Result<&Arc<dyn PhysicalExpr>> {
        self.compiled.get_or_try_init(|| {
            let schema = self.batch_schema();
            let df_schema = DFSchema::try_from(schema)?;
            SessionContext::new().create_physical_expr(self.logical_expr.clone(), &df_schema)
        })
    }

    fn transform_array_with_outer(
        &self,
        array: &ArrayRef,
        outer_arrays: &[ArrayRef],
        return_element_field: &Arc<Field>,
    ) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::List(_) => {
                let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution("expected ListArray".to_string())
                })?;
                let transformed =
                    self.transform_values(list.values(), list.offsets(), list.len(), outer_arrays)?;
                Ok(Arc::new(ListArray::new(
                    Arc::clone(return_element_field),
                    list.offsets().clone(),
                    transformed,
                    list.nulls().cloned(),
                )))
            }
            DataType::LargeList(_) => {
                let list = array
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "expected LargeListArray".to_string(),
                        )
                    })?;
                let offsets_i32: Vec<i32> = list
                    .offsets()
                    .iter()
                    .map(|&o| {
                        i32::try_from(o).map_err(|_| {
                            datafusion_common::DataFusionError::Execution(
                                "LargeList offset overflows i32".to_string(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let offsets_i32_buf = OffsetBuffer::<i32>::new(offsets_i32.into());
                let transformed = self.transform_values(
                    list.values(),
                    &offsets_i32_buf,
                    list.len(),
                    outer_arrays,
                )?;
                let offsets_i64 = list.offsets().clone();
                Ok(Arc::new(LargeListArray::new(
                    Arc::clone(return_element_field),
                    offsets_i64,
                    transformed,
                    list.nulls().cloned(),
                )))
            }
            other => exec_err!(
                "spark_array_transform_expr expects List type, got {:?}",
                other
            ),
        }
    }

    fn transform_values(
        &self,
        values: &ArrayRef,
        offsets: &OffsetBuffer<i32>,
        n_rows: usize,
        outer_arrays: &[ArrayRef],
    ) -> Result<ArrayRef> {
        if values.is_empty() {
            return Ok(datafusion::arrow::array::new_empty_array(
                &self.return_element_type,
            ));
        }

        let arrow_schema = self.batch_schema();
        let mut columns: Vec<ArrayRef> = vec![values.clone()];
        let mut element_to_row: Vec<usize> = Vec::with_capacity(values.len());
        let mut indices_for_index_col: Vec<i32> = Vec::with_capacity(values.len());

        for row_idx in 0..n_rows {
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;
            for i in 0..(end - start) {
                element_to_row.push(row_idx);
                indices_for_index_col.push(i as i32);
            }
        }

        if self.index_column_name.is_some() {
            columns.push(Arc::new(Int32Array::from(indices_for_index_col)));
        }

        for outer_arr in outer_arrays {
            let take_indices = datafusion::arrow::array::UInt64Array::from(
                element_to_row.iter().map(|&i| i as u64).collect::<Vec<_>>(),
            );
            let broadcast =
                datafusion::arrow::compute::take(outer_arr.as_ref(), &take_indices, None)?;
            columns.push(broadcast);
        }

        let batch = RecordBatch::try_new(arrow_schema, columns)?;
        let result_cv = self.physical_expr()?.evaluate(&batch)?;
        result_cv.into_array(values.len())
    }
}

impl Clone for SparkArrayTransformExpr {
    fn clone(&self) -> Self {
        let num_args = 1 + self.outer_columns.len();
        let compiled = OnceCell::new();
        if let Some(p) = self.compiled.get() {
            let _ = compiled.set(Arc::clone(p));
        }
        Self {
            signature: Signature::any(num_args, Volatility::Immutable),
            logical_expr: self.logical_expr.clone(),
            compiled,
            element_type: self.element_type.clone(),
            return_element_type: self.return_element_type.clone(),
            column_name: self.column_name.clone(),
            index_column_name: self.index_column_name.clone(),
            outer_columns: self.outer_columns.clone(),
        }
    }
}

impl PartialEq for SparkArrayTransformExpr {
    fn eq(&self, other: &Self) -> bool {
        self.logical_expr == other.logical_expr
            && self.element_type == other.element_type
            && self.return_element_type == other.return_element_type
            && self.column_name == other.column_name
            && self.index_column_name == other.index_column_name
            && self.outer_columns == other.outer_columns
    }
}

impl std::hash::Hash for SparkArrayTransformExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        format!("{:?}", self.logical_expr).hash(state);
        self.element_type.hash(state);
        self.return_element_type.hash(state);
        self.column_name.hash(state);
        self.index_column_name.hash(state);
        self.outer_columns.hash(state);
    }
}

impl Eq for SparkArrayTransformExpr {}

impl ScalarUDFImpl for SparkArrayTransformExpr {
    fn name(&self) -> &str {
        "spark_array_transform_expr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let elem_field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            self.return_element_type.clone(),
            true,
        ));
        match &arg_types[0] {
            DataType::List(_) => Ok(DataType::List(elem_field)),
            DataType::LargeList(_) => Ok(DataType::LargeList(elem_field)),
            other => exec_err!(
                "spark_array_transform_expr expects List type, got {:?}",
                other
            ),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let return_type = self.return_type(
            &args
                .arg_fields
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(Arc::new(Field::new(self.name(), return_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, return_field, ..
        } = args;

        let expected_args = 1 + self.outer_columns.len();
        if args.len() != expected_args {
            return exec_err!(
                "spark_array_transform_expr requires {} arguments (array + {} outer columns), got {}",
                expected_args,
                self.outer_columns.len(),
                args.len()
            );
        }

        let array_arg = match &args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array_of_size(1)?,
        };

        let outer_arrays: Vec<ArrayRef> = args[1..]
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(arr) => Ok(arr.clone()),
                ColumnarValue::Scalar(s) => s.to_array_of_size(array_arg.len()),
            })
            .collect::<Result<Vec<_>>>()?;

        let return_element_field = match return_field.data_type() {
            DataType::List(f) | DataType::LargeList(f) => Arc::clone(f),
            other => {
                return exec_err!(
                    "spark_array_transform_expr expected return_field to be a list, got {}",
                    other
                )
            }
        };

        let result =
            self.transform_array_with_outer(&array_arg, &outer_arrays, &return_element_field)?;
        Ok(ColumnarValue::Array(result))
    }
}
