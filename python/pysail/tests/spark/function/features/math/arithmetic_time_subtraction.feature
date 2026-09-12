Feature: TIME subtraction result parity

  # `SubtractTimes` returns `DayTimeIntervalType(HOUR, SECOND)` (`timeExpressions.scala:626`).
  # Sail casts the difference to Arrow `Duration`, which keeps the value and the day-time
  # family but cannot carry the HOUR TO SECOND start/end fields.
  @sail-bug
  @spark-4.1
  Scenario: TIME subtraction preserves Spark's HOUR TO SECOND interval subtype
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT
        typeof(
          CAST(TIME '12:00:00.000001' AS TIME(6)) -
          CAST(TIME '12:00:01.250' AS TIME(3))
        ) AS result_type,
        CAST(
          CAST(TIME '12:00:00.000001' AS TIME(6)) -
          CAST(TIME '12:00:01.250' AS TIME(3))
          AS STRING
        ) AS result
      """
    Then query result
      | result_type             | result                                     |
      | interval hour to second | INTERVAL '-00:00:01.249999' HOUR TO SECOND |

  # A TIME difference is a day-time interval, so Spark feeds it straight back into
  # `TimeAddInterval` (`BinaryArithmeticWithDatetimeResolver.scala:87,133`). Sail spells that
  # interval as Arrow `Duration`, which DataFusion's `time +- interval` rule does not match.
  @spark-4.1
  Scenario: a TIME difference composes back with a TIME
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT
        CAST(TIME '12:00:00' + (TIME '12:00:00' - TIME '01:00:00') AS STRING) AS added,
        CAST(TIME '12:00:00' - (TIME '12:00:00' - TIME '01:00:00') AS STRING) AS subtracted
      """
    Then query result
      | added    | subtracted |
      | 23:00:00 | 01:00:00   |

  @spark-4.1
  Scenario: a TIME takes a day-time interval in either order
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT
        CAST(TIME '12:00:00' + INTERVAL '1' HOUR AS STRING) AS a,
        CAST(INTERVAL '1' HOUR + TIME '12:00:00' AS STRING) AS b,
        CAST(TIME '12:00:00' - INTERVAL '1' HOUR AS STRING) AS c
      """
    Then query result
      | a        | b        | c        |
      | 13:00:00 | 13:00:00 | 11:00:00 |

  # NOT this PR's work -- the fix belongs with the ANSI/overflow PR. Pinned here only because the
  # `TIME +- interval` arms above turn a hard error into a WRONG VALUE: DataFusion wraps within
  # the 24-hour clock, Spark raises `[DATETIME_OVERFLOW]` in both ANSI modes.
  @sail-bug
  @spark-4.1
  Scenario: TIME arithmetic that leaves the day overflows
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TIME '23:30:00' + INTERVAL '2' HOUR AS result
      """
    Then query error (?i)DATETIME_OVERFLOW

