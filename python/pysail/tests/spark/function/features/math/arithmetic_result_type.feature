Feature: arithmetic result types (+ - * / %) vs Spark 4.2.0

  # `arithmetic_operand_resolution.feature` asserts that a pair RESOLVES; this pins WHAT TYPE it
  # resolves to, which that file declares out of its scope. Measured 2026-09-12 across its 1797
  # resolving cells against Spark 4.2.0: 1177 agree and 225 differ, in 33 shapes. One scenario per
  # root cause rather than per spelling, so 13 rows stand for the 225. None is fixed here -- the
  # operand-rejection work does not touch the coercion contract -- so every one is `@sail-bug`.

  Rule: an interval keeps the field range it was declared with

    # Spark's `YearMonthIntervalType`/`DayTimeIntervalType` carry `start_field` and `end_field`
    # (`YearMonthIntervalType.scala:50-59`, `DayTimeIntervalType.scala:54-63`) and name the range
    # they were declared with. Sail's Arrow types cannot carry them, so every year-month interval
    # reads YEAR TO MONTH and every day-time one DAY TO SECOND. The metadata that would fix it
    # exists only on the `fix/interval` branch, so this is the largest family that cannot be
    # closed from here: 56 of the 225 cells.
    @sail-bug
    Scenario Outline: <case> keeps its field range
      When query
        """
        SELECT typeof(<expression>) AS result
        """
      Then query result
        | result |
        | <type> |

      Examples:
        | case          | expression                              | type                 |
        | month + month | INTERVAL '2' MONTH + INTERVAL '1' MONTH | interval month       |
        | year + year   | INTERVAL '2' YEAR + INTERVAL '1' YEAR   | interval year        |
        | day + day     | INTERVAL '2' DAY + INTERVAL '1' DAY     | interval day         |
        | hour + hour   | INTERVAL '25' HOUR + INTERVAL '1' HOUR  | interval hour        |
        | day + hour    | INTERVAL '2' DAY + INTERVAL '25' HOUR   | interval day to hour |

  Rule: subtracting two datetimes yields an interval

    # `SubtractDates` returns `DayTimeIntervalType(DAY)` (`datetimeExpressions.scala:3617`) and
    # `SubtractTimestamps` returns `DayTimeIntervalType()`. Sail returns a plain BIGINT for the
    # first and an Arrow `Duration` for the second. The BIGINT is not cosmetic: it is why
    # `is_date_offset_numeric` cannot be narrowed to Spark's accept set, since refusing a BIGINT
    # offset would refuse `DATE + datediff(...)`, which Spark answers.
    @sail-bug
    Scenario Outline: <case> is an interval
      When query
        """
        SELECT typeof(<expression>) AS result
        """
      Then query result
        | result |
        | <type> |

      Examples:
        | case        | expression                                        | type                   |
        | date - date | DATE'2024-01-15' - DATE'2024-01-01'               | interval day           |
        | date - ts   | DATE'2024-01-15' - TIMESTAMP'2024-01-01 00:00:00' | interval day to second |

  Rule: a date shifted by a day-time interval becomes a timestamp

    # `BinaryArithmeticWithDatetimeResolver.scala:69` rewrites `date + <day-time>` into
    # `TimestampAddInterval`, so the result is a TIMESTAMP even though the left operand is a DATE.
    # Sail keeps the DATE, which silently drops the time-of-day part of the interval.
    @sail-bug
    Scenario: a date plus a day-time interval is a timestamp
      When query
        """
        SELECT typeof(DATE'2024-01-15' + INTERVAL '25' HOUR) AS result
        """
      Then query result
        | result    |
        | timestamp |

  Rule: numeric coercion matches Spark's

    # Spark's `findTightestCommonType`/decimal promotion, which Sail's coercion does not reproduce
    # for these three shapes. Together they are 84 of the 225 cells.
    @sail-bug
    Scenario Outline: numeric coercion: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(<expression>) AS result
        """
      Then query result
        | result |
        | <type> |

      Examples:
        | case                              | expression                                 | type           |
        | a float with a decimal is double  | CAST(2 AS FLOAT) + CAST(2 AS DECIMAL(10,2)) | double         |
        | a modulo keeps the smallint width | CAST(2 AS SMALLINT) % CAST(2 AS SMALLINT)   | smallint       |
        | a modulo keeps the tinyint width  | CAST(2 AS TINYINT) % CAST(2 AS TINYINT)     | tinyint        |
        | a decimal division widens         | CAST(2 AS DECIMAL(10,2)) / CAST(2 AS INT)   | decimal(21,13) |

  Rule: a calendar interval divided by a number stays an interval

    # `BinaryArithmeticWithDatetimeResolver.scala:158` rewrites it into `DivideInterval`. Sail
    # coerces both sides to DOUBLE instead, losing the interval entirely.
    @sail-bug
    Scenario: a calendar interval divided by a number is an interval
      When query
        """
        SELECT typeof(make_interval(0, 1, 0, 1, 0, 0, 0) / 2) AS result
        """
      Then query result
        | result   |
        | interval |
