import pytest
from pyspark.errors import AnalysisException
from pyspark.sql.types import ArrayType, MapType, StringType, StructType

from pysail.testing.spark.utils.common import is_jvm_spark
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
        pytest.param("nvl2(a, a, a)", id="nvl2"),
        pytest.param("if(true, a, a)", id="if"),
        pytest.param("CASE WHEN true THEN a ELSE a END", id="case"),
        pytest.param("nullif(a, a)", id="nullif"),
        pytest.param("arr[0]", id="array-index"),
        pytest.param("element_at(arr, 1)", id="element_at"),
        pytest.param("m['k']", id="map-value"),
        pytest.param(
            "array(a)[0]",
            id="array-constructor",
            marks=pytest.mark.xfail(
                not is_jvm_spark(),
                strict=True,
                reason="array() builds its element field without the UDT metadata",
            ),
        ),
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


@pytest.mark.parametrize(
    "query",
    [
        pytest.param("SELECT first(a) / 1 FROM udt_relation_operand", id="first"),
        pytest.param("SELECT last(a) / 1 FROM udt_relation_operand", id="last"),
        pytest.param("SELECT any_value(a) / 1 FROM udt_relation_operand", id="any_value"),
        pytest.param("SELECT lag(a) OVER (ORDER BY k) / 1 FROM udt_relation_operand", id="lag"),
        pytest.param("SELECT lead(a) OVER (ORDER BY k) / 1 FROM udt_relation_operand", id="lead"),
        pytest.param("SELECT first_value(a) OVER (ORDER BY k) / 1 FROM udt_relation_operand", id="first_value"),
        pytest.param(
            "SELECT e / 1 FROM (SELECT explode(arr) AS e FROM udt_relation_operand)",
            id="explode",
            marks=pytest.mark.xfail(
                not is_jvm_spark(),
                strict=True,
                reason="the generator builds its output column without the UDT metadata",
            ),
        ),
        pytest.param(
            "SELECT u / 1 FROM "
            "(SELECT a AS u FROM udt_relation_operand UNION ALL SELECT a AS u FROM udt_relation_operand)",
            id="union",
        ),
        pytest.param(
            "SELECT max_by(a, k) / 1 FROM udt_relation_operand",
            id="max_by",
            marks=pytest.mark.xfail(
                not is_jvm_spark(),
                strict=True,
                reason="max_by builds its result field without the UDT metadata",
            ),
        ),
    ],
)
def test_udt_reached_through_an_aggregate_window_or_relation_is_rejected(spark, query):
    # A UDT that comes out of an aggregate, a window function, a generator or a set operation is
    # still a UDT in Spark, so `/ 1` fails analysis.
    udt = UnnamedPythonUDT()
    schema = StructType().add("k", "integer").add("a", udt).add("arr", ArrayType(udt))
    spark.createDataFrame(data=[], schema=schema).createOrReplaceTempView("udt_relation_operand")
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        spark.sql(query).collect()


@pytest.mark.parametrize("ansi_enabled", ["false", "true"])
@pytest.mark.parametrize(
    "expression",
    [
        pytest.param("CAST(a AS STRING) / 1", id="cast"),
        pytest.param("TRY_CAST(a AS STRING) / 1", id="try_cast"),
        pytest.param("coalesce(CAST(a AS STRING), '0') / 1", id="coalesce-of-cast"),
        pytest.param("CAST(CAST(a AS STRING) AS INT) + 1", id="cast-to-int"),
    ],
)
def test_udt_cast_to_a_plain_type_is_a_plain_operand(spark, udt_view, ansi_enabled, expression):
    # A cast yields its target type, so `CAST(udt AS STRING)` is a plain STRING in Spark and the
    # arithmetic resolves. DataFusion copies the source field's metadata onto the cast, so the
    # guard must not read the UDT marker through it.
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi_enabled)
    try:
        assert spark.sql(f"SELECT {expression} AS r FROM {udt_view}").collect() == []  # noqa: S608
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="a column projected from a cast keeps the UDT metadata in the plan schema",
)
def test_udt_cast_projected_by_a_subquery_is_a_plain_operand(spark, udt_view):
    # The cast's output field must not carry the UDT metadata, or the column a subquery projects
    # from it is judged a UDT and arithmetic Spark resolves is rejected. Neutralising the marker
    # through the alias metadata does not work: the physical plan rebuilds the field from the
    # source and Sail then reports `Schema field unallowed change`.
    query = f"SELECT x / 1 AS r FROM (SELECT CAST(a AS STRING) AS x FROM {udt_view})"  # noqa: S608
    assert spark.sql(query).collect() == []
