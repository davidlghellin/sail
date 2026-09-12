"""`spark.sql.timeType.enabled`, the flag that hides the TIME type.

Spark keeps TIME off outside its own tests and refuses it in every Connect execution, not only
when converting to Arrow. Sail implements TIME and keeps it on, so the flag is what a user sets
to get Spark's answer.

The flag is tested here rather than in a `.feature` because a scenario has to leave the session
as it found it: these tests unset the key, which is what tells "unset" from "set to the default".
"""

import contextlib

import pytest
from pyspark.errors import PySparkException

from pysail.testing.spark.utils.common import is_jvm_spark

TIME_EXPRESSIONS = [
    pytest.param("TIME'01:02:03'", id="literal"),
    pytest.param("CAST('01:02:03' AS TIME(6))", id="cast"),
    pytest.param("CAST('01:02:03' AS TIME(0))", id="cast-to-time32"),
]

# Spark maps every TIME precision to Arrow `time64[ns]` (`ArrowUtils.scala`), so a client can
# always decode it. Sail sends `time32[s]` for TIME(0), which PySpark refuses outright.
RESOLVING_TIME_EXPRESSIONS = [
    *TIME_EXPRESSIONS[:2],
    pytest.param(
        "CAST('01:02:03' AS TIME(0))",
        id="cast-to-time32",
        marks=pytest.mark.xfail(
            not is_jvm_spark(),
            strict=True,
            reason="Sail sends TIME(0) as `time32[s]`, which the client cannot convert to Arrow",
        ),
    ),
]


@contextlib.contextmanager
def time_type_enabled(spark, value):
    spark.conf.set("spark.sql.timeType.enabled", value)
    try:
        yield
    finally:
        spark.conf.unset("spark.sql.timeType.enabled")


@pytest.mark.parametrize("expression", TIME_EXPRESSIONS)
def test_the_time_type_is_refused_when_the_flag_is_off(spark, expression):
    with (
        time_type_enabled(spark, "false"),
        pytest.raises(PySparkException, match="(?i)the data type TIME is not supported"),
    ):
        spark.sql(f"SELECT {expression} AS result").collect()  # noqa: S608


@pytest.mark.parametrize("expression", RESOLVING_TIME_EXPRESSIONS)
def test_the_time_type_resolves_when_the_flag_is_on(spark, expression):
    with time_type_enabled(spark, "true"):
        assert spark.sql(f"SELECT {expression} AS result").collect()[0][0].isoformat() == "01:02:03"  # noqa: S608


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="Sail keeps the TIME type on by default, where Spark keeps it off",
)
def test_the_time_type_is_refused_by_default(spark):
    # The deliberate superset: Sail answers where Spark refuses. Pinned so the day the default
    # changes -- or the day Spark turns the flag on -- this says so.
    with pytest.raises(PySparkException, match="(?i)the data type TIME is not supported"):
        spark.sql("SELECT TIME'01:02:03' AS result").collect()
