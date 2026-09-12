Feature: arithmetic operands whose type is derived, vs Spark 4.2.0

  # The blind spot of `arithmetic_operand_rejection.feature`. That matrix enumerates 28 tokens,
  # and every one of its 12088 operands is a literal or a type constructor, so BOTH ENGINES
  # ALWAYS LAND IN THE SAME CELL. When the operand is an expression whose result type differs
  # between the engines, the same query lands in a different cell in each, and no cell of that
  # matrix can see it -- which is how `DATE + datediff(...)` was refused for a while.
  #
  # Found by brute force, 2026-09-12: the 910 executable examples in the function catalogue were
  # run through `typeof` on both engines (55 differ), and the 55 divergent expressions were then
  # combined with a DATE and an INT operand under all five operators (550 cells, 38 verdicts
  # differ). One scenario per root cause. Each row was measured on Spark first and then on Sail.

  Rule: a date offset that comes out of a function resolves

    # The regression this file exists for. Spark types `datediff` and `regexp_count` as INT, Sail
    # as BIGINT, so a guard narrowed to Spark's accept set refuses a query Spark answers once the
    # value crosses a projection boundary. Green on both engines; it locks the accept set open.
    Scenario Outline: a date shifted by <case> resolves
      When query
        """
        SELECT CAST(DATE'2024-01-15' + <offset> AS STRING) AS result
        """
      Then query result
        | result     |
        | <expected> |

      Examples:
        | case              | offset                                       | expected   |
        | a regexp count    | regexp_count('aaa', 'a')                     | 2024-01-18 |
        | a date difference | datediff(DATE'2024-01-20', DATE'2024-01-15') | 2024-01-20 |

  Rule: a BINARY-returning function is a BINARY operand

    # Spark types `substr`/`substring`/`left`/`overlay` over a BINARY input as BINARY, and BINARY
    # is not an arithmetic operand, so it refuses at ANALYSIS. Sail types them as STRING, which
    # `/` and `%` happily coerce, so the query is planned and then dies at RUNTIME with
    # `Cannot cast string 'k SQL' to value of Int32`. Later, and with a worse message.
    @sail-bug
    Scenario Outline: <case> is refused as an arithmetic operand
      When query
        """
        SELECT CAST(2 AS INT) <op> <operand> AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | case                | op | operand                                                              |
        | substr of a binary  | /  | substr(encode('Spark SQL', 'utf-8'), 5)                              |
        | substr of a binary  | %  | substr(encode('Spark SQL', 'utf-8'), 5)                              |
        | substring of binary | /  | substring(encode('Spark SQL', 'utf-8'), 5)                           |
        | left of a binary    | /  | left(encode('Spark SQL', 'utf-8'), 3)                                |
        | overlay of a binary | /  | overlay(encode('Spark SQL', 'utf-8') PLACING encode('_','utf-8') FROM 6) |

  Rule: a function returning an ARRAY is an ARRAY operand

    # Spark types `nvl(NULL, array('2'))` as `ARRAY<STRING>` and refuses it; Sail types it STRING
    # and fails at runtime instead. Same shape as the BINARY family, different return type.
    @sail-bug
    Scenario: an array from nvl is refused as an arithmetic operand
      When query
        """
        SELECT CAST(2 AS INT) / nvl(NULL, array('2')) AS result
        """
      Then query error (?i)cannot resolve

  Rule: make_date with a NULL argument is still a DATE

    # Spark keeps the DATE type and refuses the multiplication; Sail types it VOID, so the guard
    # never sees a date and the query ANSWERS `NULL`. The only row in this file where Sail returns
    # a value for a query Spark rejects outright.
    @sail-bug
    Scenario: a date built with a NULL argument is refused as an arithmetic operand
      When query
        """
        SELECT CAST(2 AS INT) * make_date(2019, 7, NULL) AS result
        """
      Then query error (?i)cannot resolve

  Rule: the result type of a function decides the cell it lands in

    # The root of every row above, asserted directly. `bitmap_bit_position` runs the other way
    # from `datediff`: Spark types it BIGINT, which `DateAdd` refuses, while Sail types it INT and
    # accepts the offset.
    @sail-bug
    Scenario Outline: <case>
      When query
        """
        SELECT typeof(<expression>) AS result
        """
      Then query result
        | result |
        | <type> |

      Examples:
        | case                            | expression               | type   |
        | regexp_instr returns an INT     | regexp_instr('abc', 'b') | int    |
        | bitmap_bit_position is a BIGINT | bitmap_bit_position(1)   | bigint |

    @sail-bug
    Scenario: a date shifted by a BIGINT-typed function is refused
      When query
        """
        SELECT DATE'2024-01-15' + bitmap_bit_position(1) AS result
        """
      Then query error (?i)cannot resolve

  Rule: a composed operand lands in the cell its own type names

    # Composition needs no matrix of its own: measured over 120 composed cells, a divergence
    # appears ONLY where the inner expression's result type already diverges (60 cells whose inner
    # type agrees produced 0), and not even always -- `float + decimal` differs in type yet stays
    # numeric, so the verdict holds. So the risk reduces to the result-type divergences already
    # pinned in `arithmetic_result_type.feature`; these four are that risk made observable.
    #
    # Sail's inner type is BIGINT where Spark's is `INTERVAL DAY`, and DATE where Spark's is
    # TIMESTAMP, so the outer operator sees a different operand in each engine.
    @sail-bug
    Scenario Outline: <case> is refused
      When query
        """
        SELECT <expression> AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | case                            | expression                                              |
        | a date difference plus an INT   | (DATE'2024-01-15' - DATE'2024-01-01') + CAST(2 AS INT)  |
        | a shifted date plus an INT      | (DATE'2024-01-15' + INTERVAL '25' HOUR) + CAST(2 AS INT) |

    # The other direction, and the worse one: Sail REFUSES what Spark answers. Only one row:
    # the other over-rejection the sweep found -- a year-month interval times a number -- is
    # already inventoried by 60 `@sail-bug` rows in `arithmetic_operand_resolution.feature`,
    # and its root is a missing implementation, not a type that differs.
    @sail-bug
    Scenario Outline: <case> resolves
      When query
        """
        SELECT CAST(<expression> AS STRING) AS result
        """
      Then query result
        | result     |
        | <expected> |

      Examples:
        | case                               | expression                                               | expected          |
        | a date difference plus an interval | (DATE'2024-01-15' - DATE'2024-01-01') + INTERVAL '2' DAY | INTERVAL '16' DAY |

