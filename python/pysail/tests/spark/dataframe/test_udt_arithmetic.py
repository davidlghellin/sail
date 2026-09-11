import pytest
from pyspark.errors import AnalysisException
from pyspark.sql.types import ArrayType, MapType, StringType, StructType

from pysail.tests.spark.dataframe.udt import UnnamedPythonUDT

OPERATORS = ["+", "-", "*", "/", "%"]


@pytest.fixture
def udt_view(spark):
    schema = StructType().add("a", UnnamedPythonUDT())
    spark.createDataFrame(data=[], schema=schema).createOrReplaceTempView("udt_arithmetic")
    return "udt_arithmetic"


@pytest.mark.parametrize("op", OPERATORS)
def test_udt_operand_is_rejected(spark, udt_view, op):
    # A UDT is none of the input types the arithmetic operators accept, whatever
    # it is stored as -- this one is stored as STRING, which on its own would let
    # `/` through.
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        spark.sql(f"SELECT a {op} 1 FROM {udt_view}").collect()  # noqa: S608


def test_udt_operand_is_named_udt_not_its_storage_type(spark, udt_view):
    # Both engines spell it exactly this way.
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"SELECT a + 1 FROM {udt_view}").collect()  # noqa: S608
    assert 'UDT("STRING")' in str(excinfo.value)


def test_nested_udt_is_named_by_its_storage_type(spark):
    # Nested, it goes the other way round: Spark names the storage type, so the
    # struct reads STRUCT<u: STRING, ...>. Pinned because the tempting "fix" --
    # naming nested fields the way top-level ones are named -- would diverge.
    schema = StructType().add("s", StructType().add("u", UnnamedPythonUDT()).add("n", "integer"))
    spark.createDataFrame(data=[], schema=schema).createOrReplaceTempView("nested_udt_arithmetic")
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql("SELECT s + 1 FROM nested_udt_arithmetic").collect()
    assert "STRUCT<u: STRING, n: INT>" in str(excinfo.value)


@pytest.mark.parametrize(
    "operand",
    [
        pytest.param("a", id="column"),
        pytest.param("s.u", id="struct-field"),
        pytest.param("coalesce(a, a)", id="coalesce"),
        pytest.param("nvl(a, a)", id="nvl"),
        pytest.param("if(true, a, a)", id="if"),
        pytest.param("CASE WHEN true THEN a ELSE a END", id="case"),
        pytest.param("nullif(a, a)", id="nullif"),
        pytest.param("arr[0]", id="array-index"),
        pytest.param("element_at(arr, 1)", id="element_at"),
        pytest.param("m['k']", id="map-value"),
    ],
)
def test_udt_reached_through_an_expression_is_rejected(spark, operand):
    # Every one of these is still a UDT in Spark, so `/ 1` fails analysis. Only the column and
    # the struct field carry the UDT metadata on their own field; the rest build their result
    # field without it, so the guard has to look through them. `/` is the operator where a
    # STRING-backed UDT would otherwise resolve, since it casts both sides to DOUBLE.
    udt = UnnamedPythonUDT()
    schema = (
        StructType()
        .add("a", udt)
        .add("s", StructType().add("u", udt))
        .add("arr", ArrayType(udt))
        .add("m", MapType(StringType(), udt))
    )
    spark.createDataFrame(data=[], schema=schema).createOrReplaceTempView("udt_expression_operand")
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        spark.sql(f"SELECT {operand} / 1 FROM udt_expression_operand").collect()  # noqa: S608
