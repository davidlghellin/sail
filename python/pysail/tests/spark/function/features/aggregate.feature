@aggregate
Feature: aggregate and reduce higher-order functions

  Rule: Basic reduce — fold array to scalar

    Scenario: Sum integers
      When query
        """
        SELECT reduce(array(1, 2, 3, 4, 5), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 15     |

    Scenario: Product of integers
      When query
        """
        SELECT reduce(array(1, 2, 3, 4, 5), 1, (acc, x) -> acc * x) AS result
        """
      Then query result
        | result |
        | 120    |

    Scenario: Single element array
      When query
        """
        SELECT reduce(array(42), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: Maximum value using greatest
      When query
        """
        SELECT reduce(array(3, 1, 4, 1, 5, 9, 2, 6), 0, (acc, x) -> greatest(acc, x)) AS result
        """
      Then query result
        | result |
        | 9      |

    Scenario: Concatenate strings
      When query
        """
        SELECT reduce(array('a', 'b', 'c'), '', (acc, x) -> concat(acc, x)) AS result
        """
      Then query result
        | result |
        | abc    |

    Scenario: Count array elements using reduce
      When query
        """
        SELECT reduce(array('x', 'y', 'z'), 0, (acc, x) -> acc + 1) AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: With finish lambda — transform accumulator after fold

    Scenario: Sum then double with finish
      When query
        """
        SELECT aggregate(array(1, 2, 3, 4), 0, (acc, x) -> acc + x, acc -> acc * 2) AS result
        """
      Then query result
        | result |
        | 20     |

    Scenario: Sum then offset with finish
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc + 100) AS result
        """
      Then query result
        | result |
        | 106    |

    Scenario: Sum then negate with finish
      When query
        """
        SELECT aggregate(array(1, 2, 3, 4, 5), 0, (acc, x) -> acc + x, acc -> -acc) AS result
        """
      Then query result
        | result |
        | -15    |

    Scenario: Integer division in finish for average
      When query
        """
        SELECT aggregate(array(10, 20, 30, 40), 0, (acc, x) -> acc + x, acc -> acc / 4) AS result
        """
      Then query result
        | result |
        | 25.0   |

  Rule: aggregate is an alias for reduce

    Scenario: aggregate with 3 arguments matches reduce
      When query
        """
        SELECT
          reduce(array(10, 20, 30), 0, (acc, x) -> acc + x) AS r,
          aggregate(array(10, 20, 30), 0, (acc, x) -> acc + x) AS a
        """
      Then query result
        | r  | a  |
        | 60 | 60 |

    Scenario: aggregate with 4 arguments applies finish
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10) AS result
        """
      Then query result
        | result |
        | 60     |

  Rule: Empty array returns initial value (with finish applied)

    Scenario: reduce on empty int array returns zero value
      When query
        """
        SELECT reduce(x, 99, (acc, e) -> acc + e) AS result
        FROM VALUES (CAST(array() AS array<int>)) AS t(x)
        """
      Then query result
        | result |
        | 99     |

    Scenario: aggregate on empty array applies finish to zero
      When query
        """
        SELECT aggregate(x, 42, (acc, e) -> acc + e, acc -> acc * 2) AS result
        FROM VALUES (CAST(array() AS array<int>)) AS t(x)
        """
      Then query result
        | result |
        | 84     |

    Scenario: reduce on empty string array returns initial string
      When query
        """
        SELECT reduce(x, 'start', (acc, e) -> concat(acc, e)) AS result
        FROM VALUES (CAST(array() AS array<string>)) AS t(x)
        """
      Then query result
        | result |
        | start  |

  Rule: NULL array input returns NULL without invoking lambdas

    Scenario: NULL array returns NULL
      When query
        """
        SELECT reduce(CAST(NULL AS array<int>), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL array with finish still returns NULL
      When query
        """
        SELECT aggregate(CAST(NULL AS array<int>), 0, (acc, x) -> acc + x, acc -> acc * 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL array from VALUES table
      When query
        """
        SELECT reduce(x, 0, (acc, e) -> acc + e) AS result
        FROM VALUES (CAST(NULL AS array<int>)) AS t(x)
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL elements propagate through the merge lambda

    Scenario: NULL element causes NULL result via addition
      When query
        """
        SELECT reduce(array(1, NULL, 3), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: coalesce in merge skips NULL elements
      When query
        """
        SELECT reduce(array(1, NULL, 3), 0, (acc, x) -> acc + coalesce(x, 0)) AS result
        """
      Then query result
        | result |
        | 4      |

    Scenario: NULL element in string concat causes NULL
      When query
        """
        SELECT reduce(array('a', NULL, 'c'), '', (acc, x) -> concat(acc, x)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: coalesce in merge skips NULL strings
      When query
        """
        SELECT reduce(array('a', NULL, 'c'), '', (acc, x) -> concat(acc, coalesce(x, ''))) AS result
        """
      Then query result
        | result |
        | ac     |

    Scenario: NULL element propagates to finish
      When query
        """
        SELECT aggregate(array(1, NULL, 3), 0, (acc, x) -> acc + x, acc -> acc * 2) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL initial value propagates through merge

    Scenario: NULL zero with strict addition returns NULL
      When query
        """
        SELECT reduce(array(1, 2, 3), CAST(NULL AS int), (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: coalesce in merge can recover from NULL zero
      When query
        """
        SELECT reduce(array(1, 2, 3), CAST(NULL AS int), (acc, x) -> coalesce(acc, 0) + x) AS result
        """
      Then query result
        | result |
        | 6      |

  Rule: Different data types

    Scenario: Reduce BIGINT array
      When query
        """
        SELECT reduce(array(1L, 2L, 3L, 4L), 0L, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 10     |

    Scenario: Reduce DOUBLE array
      When query
        """
        SELECT reduce(array(1.5, 2.5, 3.0), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 7.0    |

    Scenario: Reduce boolean array with logical OR
      When query
        """
        SELECT reduce(array(false, false, true, false), false, (acc, x) -> acc OR x) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Reduce boolean array with logical AND — all true
      When query
        """
        SELECT reduce(array(true, true, true), true, (acc, x) -> acc AND x) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Reduce boolean array with logical AND — one false
      When query
        """
        SELECT reduce(array(true, false, true), true, (acc, x) -> acc AND x) AS result
        """
      Then query result
        | result |
        | false  |

  Rule: Multiple rows (batch processing)

    Scenario: reduce applied to multiple rows
      When query
        """
        SELECT id, reduce(arr, 0, (acc, x) -> acc + x) AS total
        FROM VALUES
          (1, array(1, 2, 3)),
          (2, array(10, 20)),
          (3, array(5))
        AS t(id, arr)
        ORDER BY id
        """
      Then query result ordered
        | id | total |
        | 1  | 6     |
        | 2  | 30    |
        | 3  | 5     |

    Scenario: reduce with NULL arrays mixed in batch
      When query
        """
        SELECT id, reduce(arr, 0, (acc, x) -> acc + x) AS total
        FROM VALUES
          (1, array(1, 2, 3)),
          (2, CAST(NULL AS array<int>)),
          (3, array(7, 8))
        AS t(id, arr)
        ORDER BY id
        """
      Then query result ordered
        | id | total |
        | 1  | 6     |
        | 2  | NULL  |
        | 3  | 15    |

    Scenario: aggregate with finish applied to each row
      When query
        """
        SELECT id, aggregate(arr, 0, (acc, x) -> acc + x, acc -> acc * mult) AS result
        FROM VALUES
          (1, array(1, 2, 3), 2),
          (2, array(4, 5), 10),
          (3, array(7), 1)
        AS t(id, arr, mult)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 12     |
        | 2  | 90     |
        | 3  | 7      |

  Rule: External column references in lambda body

    Scenario: Merge lambda references outer column
      When query
        """
        SELECT reduce(arr, 0, (acc, x) -> acc + x + bonus) AS result
        FROM VALUES (array(1, 2, 3), 10) AS t(arr, bonus)
        """
      Then query result
        | result |
        | 36     |

    Scenario: Finish lambda references outer column
      When query
        """
        SELECT aggregate(arr, 0, (acc, x) -> acc + x, acc -> acc + bonus) AS result
        FROM VALUES (array(1, 2, 3), 100) AS t(arr, bonus)
        """
      Then query result
        | result |
        | 106    |

  Rule: Complex lambda expressions

    Scenario: Running minimum using IF
      When query
        """
        SELECT reduce(array(3, 1, 4, 1, 5, 9, 2, 6), 2147483647, (acc, x) -> if(x < acc, x, acc)) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: Count even elements
      When query
        """
        SELECT reduce(array(1, 2, 3, 4, 5, 6), 0, (acc, x) -> acc + if(x % 2 = 0, 1, 0)) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: Find first element greater than threshold
      When query
        """
        SELECT reduce(array(3, 7, 2, 9, 4), -1, (acc, x) -> if(acc = -1 AND x > 5, x, acc)) AS result
        """
      Then query result
        | result |
        | 7      |

    Scenario: Build string with separator
      When query
        """
        SELECT reduce(array('b', 'c', 'd'), 'a', (acc, x) -> concat(acc, ',', x)) AS result
        """
      Then query result
        | result  |
        | a,b,c,d |

  Rule: Column alias named reduce is valid

    Scenario: reduce result aliased as reduce
      When query
        """
        SELECT reduce(array(1, 2, 3), 0, (acc, x) -> acc + x) AS reduce
        """
      Then query result
        | reduce |
        | 6      |

  Rule: Empty double array with explicit zero returns initial value

    Scenario: reduce on empty double array with CAST initial value returns zero
      When query
        """
        SELECT reduce(CAST(array() AS array<double>), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 0.0    |

  Rule: finish lambda is applied even when accumulator is NULL

    Scenario: aggregate NULL initial value with coalesce finish returns finish result
      When query
        """
        SELECT aggregate(array(1, 2, 3), CAST(NULL AS int), (acc, x) -> acc + x, acc -> coalesce(acc, -999)) AS result
        """
      Then query result
        | result |
        | -999   |
