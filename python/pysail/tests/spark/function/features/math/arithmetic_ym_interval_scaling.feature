Feature: scaling a year-month interval by a number, vs Spark 4.2.0

  # `BinaryArithmeticWithDatetimeResolver.scala:154-155,167` sends `<year-month> * <number>` to
  # `MultiplyYMInterval` and `<year-month> / <number>` to `DivideYMInterval`, either operand order
  # for `*`. Sail had neither: DataFusion cannot coerce `Interval(YearMonth)` against a number, so
  # all 21 cells of the matrix were REFUSED -- queries Spark answers.
  #
  # A year-month interval is a count of months and the result is a count of months too, so a
  # fractional operand is ROUNDED, HALF_UP (`intervalExpressions.scala:615,621`). That is the part
  # a naive implementation gets wrong, so the rounding is pinned case by case below.
  #
  # ANSI is NOT an axis: every row was measured on the JVM under both modes and neither the value
  # nor the error changes -- an interval divided by zero raises with ANSI off too.

  Rule: every numeric type scales a year-month interval

    # The 21 cells of the matrix, whole: seven numeric types times `iv * n`, `n * iv` and `iv / n`.
    # `INTERVAL '1-2' YEAR TO MONTH` is 14 months, so `/ 3` is 4.67 rounded to 5 and `/ 2.5` is 5.6
    # rounded to 6 -- the integral and the fractional roundings both show up in the table.
    Scenario Outline: scaling a year-month interval: <case>
      When query
        """
        SELECT CAST(<expression> AS STRING) AS v
        """
      Then query result
        | v       |
        | <value> |

      Examples:
        | case          | expression                                                | value                         |
        | ym * tinyint  | INTERVAL '1-2' YEAR TO MONTH * CAST(3 AS TINYINT)         | INTERVAL '3-6' YEAR TO MONTH  |
        | tinyint * ym  | CAST(3 AS TINYINT) * INTERVAL '1-2' YEAR TO MONTH         | INTERVAL '3-6' YEAR TO MONTH  |
        | ym / tinyint  | INTERVAL '1-2' YEAR TO MONTH / CAST(3 AS TINYINT)         | INTERVAL '0-5' YEAR TO MONTH  |
        | ym * smallint | INTERVAL '1-2' YEAR TO MONTH * CAST(3 AS SMALLINT)        | INTERVAL '3-6' YEAR TO MONTH  |
        | smallint * ym | CAST(3 AS SMALLINT) * INTERVAL '1-2' YEAR TO MONTH        | INTERVAL '3-6' YEAR TO MONTH  |
        | ym / smallint | INTERVAL '1-2' YEAR TO MONTH / CAST(3 AS SMALLINT)        | INTERVAL '0-5' YEAR TO MONTH  |
        | ym * int      | INTERVAL '1-2' YEAR TO MONTH * CAST(3 AS INT)             | INTERVAL '3-6' YEAR TO MONTH  |
        | int * ym      | CAST(3 AS INT) * INTERVAL '1-2' YEAR TO MONTH             | INTERVAL '3-6' YEAR TO MONTH  |
        | ym / int      | INTERVAL '1-2' YEAR TO MONTH / CAST(3 AS INT)             | INTERVAL '0-5' YEAR TO MONTH  |
        | ym * bigint   | INTERVAL '1-2' YEAR TO MONTH * CAST(3 AS BIGINT)          | INTERVAL '3-6' YEAR TO MONTH  |
        | bigint * ym   | CAST(3 AS BIGINT) * INTERVAL '1-2' YEAR TO MONTH          | INTERVAL '3-6' YEAR TO MONTH  |
        | ym / bigint   | INTERVAL '1-2' YEAR TO MONTH / CAST(3 AS BIGINT)          | INTERVAL '0-5' YEAR TO MONTH  |
        | ym * float    | INTERVAL '1-2' YEAR TO MONTH * CAST(1.5 AS FLOAT)         | INTERVAL '1-9' YEAR TO MONTH  |
        | float * ym    | CAST(1.5 AS FLOAT) * INTERVAL '1-2' YEAR TO MONTH         | INTERVAL '1-9' YEAR TO MONTH  |
        | ym / float    | INTERVAL '1-2' YEAR TO MONTH / CAST(1.5 AS FLOAT)         | INTERVAL '0-9' YEAR TO MONTH  |
        | ym * double   | INTERVAL '1-2' YEAR TO MONTH * CAST(2.5 AS DOUBLE)        | INTERVAL '2-11' YEAR TO MONTH |
        | double * ym   | CAST(2.5 AS DOUBLE) * INTERVAL '1-2' YEAR TO MONTH        | INTERVAL '2-11' YEAR TO MONTH |
        | ym / double   | INTERVAL '1-2' YEAR TO MONTH / CAST(2.5 AS DOUBLE)        | INTERVAL '0-6' YEAR TO MONTH  |
        | ym * decimal  | INTERVAL '1-2' YEAR TO MONTH * CAST(1.5 AS DECIMAL(10,2)) | INTERVAL '1-9' YEAR TO MONTH  |
        | decimal * ym  | CAST(1.5 AS DECIMAL(10,2)) * INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-9' YEAR TO MONTH  |
        | ym / decimal  | INTERVAL '1-2' YEAR TO MONTH / CAST(1.5 AS DECIMAL(10,2)) | INTERVAL '0-9' YEAR TO MONTH  |

  Rule: the months are rounded HALF_UP, away from zero

    Scenario Outline: rounding a scaled year-month interval: <case>
      When query
        """
        SELECT CAST(<expression> AS STRING) AS v
        """
      Then query result
        | v       |
        | <value> |

      Examples:
        | case                 | expression                                      | value                          |
        | exactly a half up    | INTERVAL '1' MONTH * CAST(2.5 AS DOUBLE)        | INTERVAL '0-3' YEAR TO MONTH   |
        | half a month is one  | INTERVAL '1' MONTH * CAST(0.5 AS DOUBLE)        | INTERVAL '0-1' YEAR TO MONTH   |
        | negative rounds away | INTERVAL '1' MONTH * CAST(-0.5 AS DOUBLE)       | INTERVAL '-0-1' YEAR TO MONTH  |
        | a decimal factor     | INTERVAL '1' MONTH * CAST(1.5 AS DECIMAL(10,2)) | INTERVAL '0-2' YEAR TO MONTH   |
        | one month halved     | INTERVAL '1' MONTH / CAST(2 AS INT)             | INTERVAL '0-1' YEAR TO MONTH   |
        | three months halved  | INTERVAL '3' MONTH / CAST(2 AS INT)             | INTERVAL '0-2' YEAR TO MONTH   |
        | divided by a half    | INTERVAL '1' MONTH / CAST(0.5 AS DOUBLE)        | INTERVAL '0-2' YEAR TO MONTH   |
        | a negative interval  | INTERVAL '-1-2' YEAR TO MONTH * CAST(2 AS INT)  | INTERVAL '-2-4' YEAR TO MONTH  |
        | times zero           | INTERVAL '1-2' YEAR TO MONTH * CAST(0 AS INT)   | INTERVAL '0-0' YEAR TO MONTH   |
        | wide but in range    | INTERVAL '1' MONTH * CAST(2000000000 AS INT)    | INTERVAL '166666666-8' YEAR TO MONTH |

  Rule: a scaled year-month interval is a year-month interval

    Scenario Outline: the type of a scaled year-month interval: <case>
      When query
        """
        SELECT typeof(<expression>) AS t
        """
      Then query result
        | t                      |
        | interval year to month |

      Examples:
        | case              | expression                                       |
        | multiplied        | INTERVAL '1-2' YEAR TO MONTH * CAST(2 AS INT)    |
        | divided           | INTERVAL '1-2' YEAR TO MONTH / CAST(2 AS INT)    |
        | a NULL factor     | INTERVAL '1-2' YEAR TO MONTH * CAST(NULL AS INT) |
        | a NULL divisor    | INTERVAL '1-2' YEAR TO MONTH / CAST(NULL AS INT) |

    Scenario Outline: a NULL operand scales to NULL: <case>
      When query
        """
        SELECT <expression> IS NULL AS v
        """
      Then query result
        | v    |
        | true |

      Examples:
        | case           | expression                                       |
        | a NULL factor  | INTERVAL '1-2' YEAR TO MONTH * CAST(NULL AS INT) |
        | a NULL divisor | INTERVAL '1-2' YEAR TO MONTH / CAST(NULL AS INT) |

  Rule: scaling a year-month interval past its bounds is an error, ANSI or not

    # The interval divisions do not read the ANSI flag at all (`IntervalDivide`), so
    # `INTERVAL_DIVIDED_BY_ZERO` is raised with ANSI off too -- unlike a numeric `/`, which returns
    # NULL there. Both modes are asserted for exactly that reason.
    Scenario Outline: scaling out of bounds: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> AS v
        """
      Then query error <error>

      Examples:
        | case                   | ansi  | expression                                      | error                |
        | divided by zero off    | false | INTERVAL '1' MONTH / CAST(0 AS INT)             | (?i)division by zero |
        | divided by zero on     | true  | INTERVAL '1' MONTH / CAST(0 AS INT)             | (?i)division by zero |
        | divided by zero double | false | INTERVAL '1' MONTH / CAST(0 AS DOUBLE)          | (?i)division by zero |
        | divided by zero on dbl | true  | INTERVAL '1' MONTH / CAST(0 AS DOUBLE)          | (?i)division by zero |
        | overflowing bigint off | false | INTERVAL '10' YEAR * CAST(9000000000 AS BIGINT) | (?i)overflow         |
        | overflowing bigint on  | true  | INTERVAL '10' YEAR * CAST(9000000000 AS BIGINT) | (?i)overflow         |
        | out of range double    | false | INTERVAL '1' MONTH * CAST(1e18 AS DOUBLE)       | (?i)out of range     |
        | out of range on        | true  | INTERVAL '1' MONTH * CAST(1e18 AS DOUBLE)       | (?i)out of range     |
        | a NaN factor           | false | INTERVAL '1' MONTH * CAST('NaN' AS DOUBLE)      | (?i)infinite or NaN  |
        | a NaN factor on        | true  | INTERVAL '1' MONTH * CAST('NaN' AS DOUBLE)      | (?i)infinite or NaN  |
