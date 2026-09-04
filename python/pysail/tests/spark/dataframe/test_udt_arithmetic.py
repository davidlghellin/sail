import pytest
from pyspark.errors import AnalysisException
from pyspark.sql.types import StructType

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
    with pytest.raises(AnalysisException, match="(?i)cannot resolve"):
        spark.sql(f"SELECT a {op} 1 FROM {udt_view}").collect()


def test_udt_operand_is_named_udt_not_its_storage_type(spark, udt_view):
    # Both engines spell it exactly this way.
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"SELECT a + 1 FROM {udt_view}").collect()
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
