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
