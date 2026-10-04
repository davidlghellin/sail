use std::sync::Arc;

use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::array::types::{
    Date32Type, Date64Type, Decimal128Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, IntervalMonthDayNanoType, IntervalYearMonthType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type,
};
use datafusion::arrow::array::{Array, ArrayRef, ArrowPrimitiveType, Int64Array, OffsetSizeTrait};
use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit, TimeUnit};
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::{DataFusionError, ScalarValue, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs};
use sail_common_datafusion::variant::is_marked_variant_storage_type;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

/// The seed of `xxhash64` in Spark.
const SEED: u64 = 42;

/// Spark's `xxhash64` function.
///
/// The encoding fed to XXH64 depends on the Spark type, and follows `HashExpression` and
/// `InterpretedHashFunction` in Spark's `hash.scala`: a NULL leaves the running hash
/// untouched, arrays and structs fold their elements with the running hash as the seed, and
/// DECIMAL with precision above 18 hashes the minimal big-endian two's complement bytes of the
/// unscaled value.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkXxhash64 {
    signature: Signature,
    /// `spark.sql.legacy.allowHashOnMapType`
    allow_hash_on_map_type: bool,
}

impl SparkXxhash64 {
    pub fn new(allow_hash_on_map_type: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            allow_hash_on_map_type,
        }
    }

    pub fn allow_hash_on_map_type(&self) -> bool {
        self.allow_hash_on_map_type
    }
}

impl ScalarUDFImpl for SparkXxhash64 {
    fn name(&self) -> &str {
        "spark_xxhash64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        // The hash of NULL is the seed, so the result is never NULL.
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, false)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return Err(DataFusionError::Plan(
                "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `xxhash64` requires > 0 parameters \
                 but the actual number is 0."
                    .to_string(),
            ));
        }
        if !self.allow_hash_on_map_type && arg_types.iter().any(contains_map) {
            return Err(DataFusionError::Plan(
                "[DATATYPE_MISMATCH.HASH_MAP_TYPE] Input to the function `xxhash64` cannot \
                 contain elements of the \"MAP\" type. In Spark, same maps may have different \
                 hashcode, thus hash expressions are prohibited on \"MAP\" elements. To restore \
                 previous behavior set \"spark.sql.legacy.allowHashOnMapType\" to \"true\"."
                    .to_string(),
            ));
        }
        if arg_types.iter().any(contains_variant) {
            return Err(DataFusionError::Plan(
                "[DATATYPE_MISMATCH.HASH_VARIANT_TYPE] Input to the function `xxhash64` cannot \
                 contain elements of the \"VARIANT\" type yet."
                    .to_string(),
            ));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.is_empty() {
            return Err(invalid_arg_count_exec_err("xxhash64", (1, i32::MAX), 0));
        }
        let all_scalar = args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let num_rows = args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);
        let arrays = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(array) => Ok(Arc::clone(array)),
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows),
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        let hashes = xxhash64_arrays(&arrays, num_rows)?;
        if all_scalar {
            let hash = hashes.first().copied().unwrap_or(SEED);
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(hash as i64))))
        } else {
            // The hashes are accumulated as `u64`; `u64` and `i64` share their layout, so the `Vec` is
            // handed to Arrow as the values of an `Int64Array` without copying.
            let values = ScalarBuffer::<i64>::from(Buffer::from_vec(hashes));
            Ok(ColumnarValue::Array(Arc::new(Int64Array::new(
                values, None,
            ))))
        }
    }
}

fn contains_map(data_type: &DataType) -> bool {
    match data_type {
        DataType::Map(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => contains_map(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| contains_map(f.data_type())),
        DataType::Dictionary(_, value) => contains_map(value),
        _ => false,
    }
}

fn contains_variant(data_type: &DataType) -> bool {
    if is_marked_variant_storage_type(data_type) {
        return true;
    }
    match data_type {
        DataType::Map(field, _)
        | DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => contains_variant(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| contains_variant(f.data_type())),
        DataType::Dictionary(_, value) => contains_variant(value),
        _ => false,
    }
}

/// Spark hashes `-0.0` as `0` and `Float.floatToIntBits` collapses every NaN.
///
/// Branchless on purpose: the slot under a NULL usually holds `0.0`, so a branch on "is it zero?" flips with the null
/// pattern and is mispredicted (1.24× `datafusion-spark` with 90 % NULLs).
#[inline]
fn float32_bits(value: f32) -> i32 {
    let bits = if value.is_nan() {
        0x7fc0_0000
    } else {
        value.to_bits() as i32
    };
    // all ones keeps the bits, zero clears them: `0.0` and `-0.0` hash as 0, NaN is not equal to zero
    bits & -i32::from(value != 0.0)
}

/// Spark hashes `-0.0` as `0` and `Double.doubleToLongBits` collapses every NaN. Branchless, as `float32_bits`.
#[inline]
fn float64_bits(value: f64) -> i64 {
    let bits = if value.is_nan() {
        0x7ff8_0000_0000_0000
    } else {
        value.to_bits() as i64
    };
    bits & -i64::from(value != 0.0)
}

/// Hashes every row of `arrays`, chaining the columns left to right with the seed 42.
pub fn xxhash64_arrays(arrays: &[ArrayRef], num_rows: usize) -> Result<Vec<u64>> {
    let mut hashes = vec![SEED; num_rows];
    for (column, array) in arrays.iter().enumerate() {
        hash_column(array.as_ref(), &mut hashes, column == 0)?;
    }
    Ok(hashes)
}

/// Chains one column into the running hashes: a typed loop when there is one, the row hasher otherwise.
fn hash_column(array: &dyn Array, hashes: &mut [u64], first_column: bool) -> Result<()> {
    if hash_typed_column(array, hashes, first_column)? {
        return Ok(());
    }
    let hasher = build_hasher(array)?;
    for (row, hash) in hashes.iter_mut().enumerate() {
        *hash = hasher(row, *hash);
    }
    Ok(())
}

/// Hashes the common top-level columns with a typed loop over the values, and returns whether it did.
///
/// The row hasher of `build_hasher` pays a `Box<dyn Fn>` call per row, which costs 20-75% on primitive
/// columns against a typed loop. Nested types, and the types that need more than one step per value
/// (floats, intervals, wide decimals, ...), keep going through `build_hasher`.
fn hash_typed_column(array: &dyn Array, hashes: &mut [u64], first_column: bool) -> Result<bool> {
    match array.data_type() {
        DataType::Int8 => hash_ints::<Int8Type>(array, hashes, i32::from),
        DataType::Int16 => hash_ints::<Int16Type>(array, hashes, i32::from),
        DataType::Int32 => hash_ints::<Int32Type>(array, hashes, |v| v),
        DataType::Date32 => hash_ints::<Date32Type>(array, hashes, |v| v),
        DataType::Int64 => hash_longs::<Int64Type>(array, hashes, |v| v),
        DataType::Float32 => hash_ints::<Float32Type>(array, hashes, float32_bits),
        DataType::Float64 => hash_longs::<Float64Type>(array, hashes, float64_bits),
        DataType::Boolean => {
            let values = array.as_boolean();
            hash_rows(array, hashes, |row, hash| {
                hash_int(i32::from(values.value(row)), hash)
            });
        }
        // A dictionary that comes first hashes each distinct value once (as DataFusion does) and gathers.
        // The running hash of the first column is the seed for every row, so the per-value hash is exact.
        DataType::Dictionary(_, _) if first_column => {
            let dictionary = array.as_any_dictionary();
            let value_hashes = xxhash64_arrays(
                &[Arc::clone(dictionary.values())],
                dictionary.values().len(),
            )?;
            let keys = dictionary.normalized_keys();
            hash_rows(array, hashes, |row, _| value_hashes[keys[row]]);
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            hash_longs::<TimestampMicrosecondType>(array, hashes, |v| v)
        }
        DataType::Decimal128(precision, _) if *precision <= 18 => {
            hash_longs::<Decimal128Type>(array, hashes, |v| v as i64)
        }
        // A wide decimal hashes the minimal big-endian two's complement bytes of its unscaled value.
        DataType::Decimal128(_, _) => {
            let values = array.as_primitive::<Decimal128Type>();
            hash_rows(array, hashes, |row, hash| {
                let mut buffer = [0u8; 16];
                hash_bytes(twos_complement_bytes(values.value(row), &mut buffer), hash)
            });
        }
        // Rows are independent, so hashing a struct column by column gives the same result as row by
        // row. A struct that is NULL leaves the running hash alone, whatever its fields hold.
        DataType::Struct(_) => {
            let columns = array.as_struct().columns();
            match array.nulls() {
                Some(nulls) if nulls.null_count() > 0 => {
                    let mut chained = hashes.to_vec();
                    for column in columns {
                        hash_column(column.as_ref(), &mut chained, false)?;
                    }
                    for (row, (hash, chained)) in hashes.iter_mut().zip(chained).enumerate() {
                        if nulls.is_valid(row) {
                            *hash = chained;
                        }
                    }
                }
                _ => {
                    for column in columns {
                        hash_column(column.as_ref(), hashes, false)?;
                    }
                }
            }
        }
        DataType::List(_) => return Ok(hash_list_column::<i32>(array, hashes)),
        DataType::LargeList(_) => return Ok(hash_list_column::<i64>(array, hashes)),
        DataType::Utf8View => {
            let values = array.as_string_view();
            hash_byte_column(array, hashes, |row| values.value(row).as_bytes());
        }
        DataType::BinaryView => {
            let values = array.as_binary_view();
            hash_byte_column(array, hashes, |row| values.value(row));
        }
        DataType::FixedSizeBinary(_) => {
            let values = array.as_fixed_size_binary();
            hash_byte_column(array, hashes, |row| values.value(row));
        }
        DataType::Utf8 => {
            let values = array.as_string::<i32>();
            hash_byte_column(array, hashes, |row| values.value(row).as_bytes());
        }
        DataType::LargeUtf8 => {
            let values = array.as_string::<i64>();
            hash_byte_column(array, hashes, |row| values.value(row).as_bytes());
        }
        DataType::Binary => {
            let values = array.as_binary::<i32>();
            hash_byte_column(array, hashes, |row| values.value(row));
        }
        DataType::LargeBinary => {
            let values = array.as_binary::<i64>();
            hash_byte_column(array, hashes, |row| values.value(row));
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// Chains the elements of a list column, with a typed loop over the elements for the common element
/// types. Returns `false` when the element type has none, and the row hasher takes the column.
fn hash_list_column<O: OffsetSizeTrait>(array: &dyn Array, hashes: &mut [u64]) -> bool {
    let list = array.as_list::<O>();
    let offsets = list.value_offsets();
    let child = list.values().as_ref();
    macro_rules! fold_elements {
        ($element_hash:expr) => {{
            let element_hash = $element_hash;
            match child.nulls() {
                Some(nulls) if nulls.null_count() > 0 => {
                    hash_rows(array, hashes, |row, hash| {
                        (offsets[row].as_usize()..offsets[row + 1].as_usize()).fold(
                            hash,
                            |hash, element| {
                                if nulls.is_valid(element) {
                                    element_hash(element, hash)
                                } else {
                                    hash
                                }
                            },
                        )
                    });
                }
                // No element is NULL: nothing to check per element
                _ => {
                    hash_rows(array, hashes, |row, hash| {
                        (offsets[row].as_usize()..offsets[row + 1].as_usize())
                            .fold(hash, |hash, element| element_hash(element, hash))
                    });
                }
            }
            true
        }};
    }
    macro_rules! ints {
        ($arrow_type:ty, $to_int:expr) => {{
            let values = child.as_primitive::<$arrow_type>();
            fold_elements!(|element: usize, hash: u64| hash_int(
                $to_int(values.value(element)),
                hash
            ))
        }};
    }
    macro_rules! longs {
        ($arrow_type:ty, $to_long:expr) => {{
            let values = child.as_primitive::<$arrow_type>();
            fold_elements!(|element: usize, hash: u64| hash_long(
                $to_long(values.value(element)),
                hash
            ))
        }};
    }
    match child.data_type() {
        DataType::Int8 => ints!(Int8Type, |v: i8| i32::from(v)),
        DataType::Int16 => ints!(Int16Type, |v: i16| i32::from(v)),
        DataType::Int32 => ints!(Int32Type, |v: i32| v),
        DataType::Date32 => ints!(Date32Type, |v: i32| v),
        DataType::Float32 => ints!(Float32Type, float32_bits),
        DataType::Int64 => longs!(Int64Type, |v: i64| v),
        DataType::Float64 => longs!(Float64Type, float64_bits),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            longs!(TimestampMicrosecondType, |v: i64| v)
        }
        DataType::Decimal128(precision, _) if *precision <= 18 => {
            longs!(Decimal128Type, |v: i128| v as i64)
        }
        DataType::Boolean => {
            let values = child.as_boolean();
            fold_elements!(|element: usize, hash: u64| hash_int(
                i32::from(values.value(element)),
                hash
            ))
        }
        DataType::Utf8 => {
            let values = child.as_string::<i32>();
            fold_elements!(|element: usize, hash: u64| hash_bytes(
                values.value(element).as_bytes(),
                hash
            ))
        }
        DataType::LargeUtf8 => {
            let values = child.as_string::<i64>();
            fold_elements!(|element: usize, hash: u64| hash_bytes(
                values.value(element).as_bytes(),
                hash
            ))
        }
        DataType::Binary => {
            let values = child.as_binary::<i32>();
            fold_elements!(|element: usize, hash: u64| hash_bytes(values.value(element), hash))
        }
        DataType::LargeBinary => {
            let values = child.as_binary::<i64>();
            fold_elements!(|element: usize, hash: u64| hash_bytes(values.value(element), hash))
        }
        _ => false,
    }
}

/// Applies `hash_row` to every valid row. The closure is generic, so it is inlined: there is no call per row.
fn hash_rows(array: &dyn Array, hashes: &mut [u64], hash_row: impl Fn(usize, u64) -> u64) {
    match array.nulls() {
        Some(nulls) if nulls.null_count() > 0 => {
            for (row, hash) in hashes.iter_mut().enumerate() {
                if nulls.is_valid(row) {
                    *hash = hash_row(row, *hash);
                }
            }
        }
        _ => {
            for (row, hash) in hashes.iter_mut().enumerate() {
                *hash = hash_row(row, *hash);
            }
        }
    }
}

fn hash_ints<T: ArrowPrimitiveType>(
    array: &dyn Array,
    hashes: &mut [u64],
    to_int: impl Fn(T::Native) -> i32,
) {
    let values = array.as_primitive::<T>().values();
    match array.nulls() {
        Some(nulls) if nulls.null_count() > 0 => {
            for (row, (hash, value)) in hashes.iter_mut().zip(values.iter()).enumerate() {
                // Branchless: with random NULLs a branch per row is mispredicted. The value under a NULL is
                // hashed too and discarded, which is harmless.
                let hashed = hash_int(to_int(*value), *hash);
                *hash = if nulls.is_valid(row) { hashed } else { *hash };
            }
        }
        _ => {
            for (hash, value) in hashes.iter_mut().zip(values.iter()) {
                *hash = hash_int(to_int(*value), *hash);
            }
        }
    }
}

fn hash_longs<T: ArrowPrimitiveType>(
    array: &dyn Array,
    hashes: &mut [u64],
    to_long: impl Fn(T::Native) -> i64,
) {
    let values = array.as_primitive::<T>().values();
    match array.nulls() {
        Some(nulls) if nulls.null_count() > 0 => {
            for (row, (hash, value)) in hashes.iter_mut().zip(values.iter()).enumerate() {
                let hashed = hash_long(to_long(*value), *hash);
                *hash = if nulls.is_valid(row) { hashed } else { *hash };
            }
        }
        _ => {
            for (hash, value) in hashes.iter_mut().zip(values.iter()) {
                *hash = hash_long(to_long(*value), *hash);
            }
        }
    }
}

fn hash_byte_column<'a>(array: &dyn Array, hashes: &mut [u64], value: impl Fn(usize) -> &'a [u8]) {
    match array.nulls() {
        Some(nulls) if nulls.null_count() > 0 => {
            for (row, hash) in hashes.iter_mut().enumerate() {
                if nulls.is_valid(row) {
                    *hash = hash_bytes(value(row), *hash);
                }
            }
        }
        _ => {
            for (row, hash) in hashes.iter_mut().enumerate() {
                *hash = hash_bytes(value(row), *hash);
            }
        }
    }
}

/// Hashes the value at a row, given the running hash. The result for a NULL is the running hash.
type RowHasher<'a> = Box<dyn Fn(usize, u64) -> u64 + 'a>;

fn with_nulls<'a>(array: &'a dyn Array, hasher: impl Fn(usize, u64) -> u64 + 'a) -> RowHasher<'a> {
    match array.nulls() {
        Some(nulls) if nulls.null_count() > 0 => {
            let nulls = nulls.clone();
            Box::new(move |row, hash| {
                if nulls.is_null(row) {
                    hash
                } else {
                    hasher(row, hash)
                }
            })
        }
        _ => Box::new(hasher),
    }
}

fn int_hasher<T: ArrowPrimitiveType>(
    array: &dyn Array,
    to_int: impl Fn(T::Native) -> i32 + 'static,
) -> RowHasher<'_> {
    let values = array.as_primitive::<T>();
    with_nulls(array, move |row, hash| {
        hash_int(to_int(values.value(row)), hash)
    })
}

fn long_hasher<T: ArrowPrimitiveType>(
    array: &dyn Array,
    to_long: impl Fn(T::Native) -> i64 + 'static,
) -> RowHasher<'_> {
    let values = array.as_primitive::<T>();
    with_nulls(array, move |row, hash| {
        hash_long(to_long(values.value(row)), hash)
    })
}

fn bytes_hasher<'a, B: AsRef<[u8]> + 'a>(
    array: &'a dyn Array,
    value: impl Fn(usize) -> B + 'a,
) -> RowHasher<'a> {
    with_nulls(array, move |row, hash| {
        hash_bytes(value(row).as_ref(), hash)
    })
}

/// Microseconds of a temporal value stored with the given unit.
fn to_micros(value: i64, unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => value.wrapping_mul(1_000_000),
        TimeUnit::Millisecond => value.wrapping_mul(1_000),
        TimeUnit::Microsecond => value,
        TimeUnit::Nanosecond => value.div_euclid(1_000),
    }
}

fn build_hasher(array: &dyn Array) -> Result<RowHasher<'_>> {
    Ok(match array.data_type() {
        DataType::Null => Box::new(|_, hash| hash),
        DataType::Boolean => {
            let values = array.as_boolean();
            with_nulls(array, move |row, hash| {
                hash_int(i32::from(values.value(row)), hash)
            })
        }
        DataType::Int8 => int_hasher::<Int8Type>(array, i32::from),
        DataType::Int16 => int_hasher::<Int16Type>(array, i32::from),
        DataType::Int32 => int_hasher::<Int32Type>(array, |v| v),
        DataType::UInt8 => int_hasher::<UInt8Type>(array, i32::from),
        DataType::UInt16 => int_hasher::<UInt16Type>(array, i32::from),
        DataType::UInt32 => long_hasher::<UInt32Type>(array, i64::from),
        DataType::Date32 => int_hasher::<Date32Type>(array, |v| v),
        DataType::Int64 => long_hasher::<Int64Type>(array, |v| v),
        DataType::Date64 => long_hasher::<Date64Type>(array, |v| v),
        DataType::Float32 => {
            let values = array.as_primitive::<Float32Type>();
            with_nulls(array, move |row, hash| {
                hash_int(float32_bits(values.value(row)), hash)
            })
        }
        DataType::Float64 => {
            let values = array.as_primitive::<Float64Type>();
            with_nulls(array, move |row, hash| {
                hash_long(float64_bits(values.value(row)), hash)
            })
        }
        // TIMESTAMP and TIMESTAMP_NTZ hash the microseconds since the epoch.
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                long_hasher::<TimestampSecondType>(array, |v| to_micros(v, &TimeUnit::Second))
            }
            TimeUnit::Millisecond => long_hasher::<TimestampMillisecondType>(array, |v| {
                to_micros(v, &TimeUnit::Millisecond)
            }),
            TimeUnit::Microsecond => long_hasher::<TimestampMicrosecondType>(array, |v| v),
            TimeUnit::Nanosecond => long_hasher::<TimestampNanosecondType>(array, |v| {
                to_micros(v, &TimeUnit::Nanosecond)
            }),
        },
        // INTERVAL DAY TO SECOND hashes the microseconds.
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => {
                long_hasher::<DurationSecondType>(array, |v| to_micros(v, &TimeUnit::Second))
            }
            TimeUnit::Millisecond => long_hasher::<DurationMillisecondType>(array, |v| {
                to_micros(v, &TimeUnit::Millisecond)
            }),
            TimeUnit::Microsecond => long_hasher::<DurationMicrosecondType>(array, |v| v),
            TimeUnit::Nanosecond => long_hasher::<DurationNanosecondType>(array, |v| {
                to_micros(v, &TimeUnit::Nanosecond)
            }),
        },
        // TIME hashes the nanoseconds since midnight, whatever its precision.
        DataType::Time32(TimeUnit::Second) => {
            long_hasher::<Time32SecondType>(array, |v| i64::from(v).wrapping_mul(1_000_000_000))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            long_hasher::<Time32MillisecondType>(array, |v| i64::from(v).wrapping_mul(1_000_000))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            long_hasher::<Time64MicrosecondType>(array, |v| v.wrapping_mul(1_000))
        }
        DataType::Time64(TimeUnit::Nanosecond) => long_hasher::<Time64NanosecondType>(array, |v| v),
        // INTERVAL YEAR TO MONTH hashes the months as an int.
        DataType::Interval(IntervalUnit::YearMonth) => {
            int_hasher::<IntervalYearMonthType>(array, |v| v)
        }
        // The calendar interval chains the microseconds, the days and the months.
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let values = array.as_primitive::<IntervalMonthDayNanoType>();
            with_nulls(array, move |row, hash| {
                let value = values.value(row);
                let micros = value.nanoseconds.div_euclid(1_000);
                hash_int(value.months, hash_int(value.days, hash_long(micros, hash)))
            })
        }
        DataType::Decimal128(precision, _) => {
            let values = array.as_primitive::<datafusion::arrow::datatypes::Decimal128Type>();
            if *precision <= 18 {
                with_nulls(array, move |row, hash| {
                    hash_long(values.value(row) as i64, hash)
                })
            } else {
                with_nulls(array, move |row, hash| {
                    let mut buffer = [0u8; 16];
                    hash_bytes(twos_complement_bytes(values.value(row), &mut buffer), hash)
                })
            }
        }
        DataType::Utf8 => {
            let values = array.as_string::<i32>();
            bytes_hasher(array, move |row| values.value(row).as_bytes())
        }
        DataType::LargeUtf8 => {
            let values = array.as_string::<i64>();
            bytes_hasher(array, move |row| values.value(row).as_bytes())
        }
        DataType::Utf8View => {
            let values = array.as_string_view();
            bytes_hasher(array, move |row| values.value(row).as_bytes())
        }
        DataType::Binary => {
            let values = array.as_binary::<i32>();
            bytes_hasher(array, move |row| values.value(row))
        }
        DataType::LargeBinary => {
            let values = array.as_binary::<i64>();
            bytes_hasher(array, move |row| values.value(row))
        }
        DataType::BinaryView => {
            let values = array.as_binary_view();
            bytes_hasher(array, move |row| values.value(row))
        }
        DataType::FixedSizeBinary(_) => {
            let values = array.as_fixed_size_binary();
            bytes_hasher(array, move |row| values.value(row))
        }
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_any_dictionary();
            let keys = dictionary.normalized_keys();
            let inner = build_hasher(dictionary.values().as_ref())?;
            with_nulls(array, move |row, hash| inner(keys[row], hash))
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            let inner = build_hasher(list.values().as_ref())?;
            with_nulls(array, move |row, hash| {
                (offsets[row] as usize..offsets[row + 1] as usize)
                    .fold(hash, |hash, element| inner(element, hash))
            })
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let offsets = list.value_offsets();
            let inner = build_hasher(list.values().as_ref())?;
            with_nulls(array, move |row, hash| {
                (offsets[row] as usize..offsets[row + 1] as usize)
                    .fold(hash, |hash, element| inner(element, hash))
            })
        }
        DataType::FixedSizeList(_, size) => {
            let list = array.as_fixed_size_list();
            let size = *size as usize;
            let inner = build_hasher(list.values().as_ref())?;
            with_nulls(array, move |row, hash| {
                let start = list.value_offset(row) as usize;
                (start..start + size).fold(hash, |hash, element| inner(element, hash))
            })
        }
        DataType::Struct(_) => {
            let fields = array
                .as_struct()
                .columns()
                .iter()
                .map(|column| build_hasher(column.as_ref()))
                .collect::<Result<Vec<_>>>()?;
            with_nulls(array, move |row, hash| {
                fields.iter().fold(hash, |hash, field| field(row, hash))
            })
        }
        // Only reachable with `spark.sql.legacy.allowHashOnMapType`: each entry hashes the key
        // and then the value.
        DataType::Map(_, _) => {
            let map = array.as_map();
            let offsets = map.value_offsets();
            let keys = build_hasher(map.keys().as_ref())?;
            let values = build_hasher(map.values().as_ref())?;
            with_nulls(array, move |row, hash| {
                (offsets[row] as usize..offsets[row + 1] as usize)
                    .fold(hash, |hash, entry| values(entry, keys(entry, hash)))
            })
        }
        other => {
            return Err(unsupported_data_type_exec_err(
                "xxhash64",
                "a type that Spark can hash",
                other,
            ));
        }
    })
}

/// The shortest big-endian two's complement representation of a 128-bit integer, which is what
/// `BigInteger.toByteArray` returns in Java. The bytes are written to `buffer` and the tail is returned.
fn twos_complement_bytes(value: i128, buffer: &mut [u8; 16]) -> &[u8] {
    *buffer = value.to_be_bytes();
    // the leading bits equal to the sign bit are redundant, but the sign bit itself is kept
    let leading = (value ^ (value >> 127)).leading_zeros();
    let length = ((128 - leading) / 8 + 1) as usize;
    &buffer[16 - length..]
}

// [Credit]: <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/XXH64.java>
// The XXH64 primitives below (`hash_int`, `hash_long`, `hash_bytes`) are a port of Spark's `XXH64`.
const PRIME64_1: u64 = 0x9E37_79B1_85EB_CA87;
const PRIME64_2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const PRIME64_3: u64 = 0x1656_67B1_9E37_79F9;
const PRIME64_4: u64 = 0x85EB_CA77_C2B2_AE63;
const PRIME64_5: u64 = 0x27D4_EB2F_1656_67C5;

#[inline]
fn fmix(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(PRIME64_2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(PRIME64_3);
    hash ^= hash >> 32;
    hash
}

#[inline]
fn round(acc: u64, input: u64) -> u64 {
    acc.wrapping_add(input.wrapping_mul(PRIME64_2))
        .rotate_left(31)
        .wrapping_mul(PRIME64_1)
}

#[inline]
fn merge_round(hash: u64, value: u64) -> u64 {
    (hash ^ round(0, value))
        .wrapping_mul(PRIME64_1)
        .wrapping_add(PRIME64_4)
}

#[inline]
fn read_u64(bytes: &[u8]) -> u64 {
    let mut word = [0u8; 8];
    word.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(word)
}

#[inline]
fn read_u32(bytes: &[u8]) -> u32 {
    let mut word = [0u8; 4];
    word.copy_from_slice(&bytes[..4]);
    u32::from_le_bytes(word)
}

/// `XXH64.hashInt` in Spark.
#[inline]
pub fn hash_int(input: i32, seed: u64) -> u64 {
    let mut hash = seed.wrapping_add(PRIME64_5).wrapping_add(4);
    hash ^= u64::from(input as u32).wrapping_mul(PRIME64_1);
    hash = hash
        .rotate_left(23)
        .wrapping_mul(PRIME64_2)
        .wrapping_add(PRIME64_3);
    fmix(hash)
}

/// `XXH64.hashLong` in Spark.
#[inline]
pub fn hash_long(input: i64, seed: u64) -> u64 {
    let mut hash = seed.wrapping_add(PRIME64_5).wrapping_add(8);
    hash ^= (input as u64)
        .wrapping_mul(PRIME64_2)
        .rotate_left(31)
        .wrapping_mul(PRIME64_1);
    hash = hash
        .rotate_left(27)
        .wrapping_mul(PRIME64_1)
        .wrapping_add(PRIME64_4);
    fmix(hash)
}

/// `XXH64.hashUnsafeBytes` in Spark.
pub fn hash_bytes(bytes: &[u8], seed: u64) -> u64 {
    let length = bytes.len();
    let mut rest = bytes;
    let mut hash = if length >= 32 {
        let mut v1 = seed.wrapping_add(PRIME64_1).wrapping_add(PRIME64_2);
        let mut v2 = seed.wrapping_add(PRIME64_2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(PRIME64_1);
        while rest.len() >= 32 {
            v1 = round(v1, read_u64(&rest[0..]));
            v2 = round(v2, read_u64(&rest[8..]));
            v3 = round(v3, read_u64(&rest[16..]));
            v4 = round(v4, read_u64(&rest[24..]));
            rest = &rest[32..];
        }
        let mut hash = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        for value in [v1, v2, v3, v4] {
            hash = merge_round(hash, value);
        }
        hash
    } else {
        seed.wrapping_add(PRIME64_5)
    };
    hash = hash.wrapping_add(length as u64);
    while rest.len() >= 8 {
        hash ^= round(0, read_u64(rest));
        hash = hash
            .rotate_left(27)
            .wrapping_mul(PRIME64_1)
            .wrapping_add(PRIME64_4);
        rest = &rest[8..];
    }
    if rest.len() >= 4 {
        hash ^= u64::from(read_u32(rest)).wrapping_mul(PRIME64_1);
        hash = hash
            .rotate_left(23)
            .wrapping_mul(PRIME64_2)
            .wrapping_add(PRIME64_3);
        rest = &rest[4..];
    }
    for byte in rest {
        hash ^= u64::from(*byte).wrapping_mul(PRIME64_5);
        hash = hash.rotate_left(11).wrapping_mul(PRIME64_1);
    }
    fmix(hash)
}
