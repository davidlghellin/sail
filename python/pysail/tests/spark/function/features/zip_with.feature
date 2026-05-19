@zip_with
Feature: zip_with higher-order function

  Rule: Basic behavior with equal-length arrays

    Scenario: Add corresponding integer elements
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> x + y) AS result
        """
      Then query result
        | result    |
        | [5, 7, 9] |

    Scenario: Subtract corresponding elements
      When query
        """
        SELECT zip_with(array(10, 20, 30), array(1, 2, 3), (x, y) -> x - y) AS result
        """
      Then query result
        | result      |
        | [9, 18, 27] |

    Scenario: Multiply corresponding elements
      When query
        """
        SELECT zip_with(array(2, 3, 4), array(5, 6, 7), (x, y) -> x * y) AS result
        """
      Then query result
        | result       |
        | [10, 18, 28] |

    Scenario: Single-element arrays
      When query
        """
        SELECT zip_with(array(42), array(58), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | [100]  |

    Scenario: Concatenate corresponding string elements
      When query
        """
        SELECT zip_with(array('a', 'b', 'c'), array('x', 'y', 'z'), (x, y) -> concat(x, y)) AS result
        """
      Then query result
        | result         |
        | [ax, by, cz]   |

    Scenario: Concatenate multi-character strings
      When query
        """
        SELECT zip_with(array('hello', 'world'), array(' foo', ' bar'), (x, y) -> concat(x, y)) AS result
        """
      Then query result
        | result               |
        | [hello foo, world bar] |

    Scenario: Boolean comparison lambda
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(3, 2, 1), (x, y) -> x > y) AS result
        """
      Then query result
        | result               |
        | [false, false, true] |

    Scenario: Conditional if in lambda
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(1, 3, 2), (x, y) -> if(x > y, 'left', 'right')) AS result
        """
      Then query result
        | result               |
        | [right, right, left] |

    Scenario: Lambda produces struct output
      When query
        """
        SELECT zip_with(array(1, 2), array(3, 4), (x, y) -> named_struct('sum', x + y, 'prod', x * y)) AS result
        """
      Then query result
        | result             |
        | [{4, 3}, {6, 8}]   |

    Scenario: Lambda uses only the first parameter
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> x * 2) AS result
        """
      Then query result
        | result    |
        | [2, 4, 6] |

    Scenario: Lambda uses only the second parameter
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> y + 10) AS result
        """
      Then query result
        | result         |
        | [14, 15, 16]   |

    Scenario: Lambda returns a constant
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> 99) AS result
        """
      Then query result
        | result         |
        | [99, 99, 99]   |

    Scenario: Lambda returns null always
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> null) AS result
        """
      Then query result
        | result               |
        | [NULL, NULL, NULL]   |

    Scenario: Lambda returns first argument unchanged
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> x) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

  Rule: Unequal-length arrays are padded with NULL

    Scenario: First array longer than second - missing second elements become NULL
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5), (x, y) -> x + y) AS result
        """
      Then query result
        | result         |
        | [5, 7, NULL]   |

    Scenario: Second array longer than first - missing first elements become NULL
      When query
        """
        SELECT zip_with(array(1, 2), array(4, 5, 6), (x, y) -> x + y) AS result
        """
      Then query result
        | result         |
        | [5, 7, NULL]   |

    Scenario: Large length difference - padded positions pass NULL to lambda
      When query
        """
        SELECT zip_with(array(1), array(1, 2, 3, 4, 5), (x, y) -> x + y) AS result
        """
      Then query result
        | result                      |
        | [2, NULL, NULL, NULL, NULL] |

    Scenario: Padded null position is typed NULL available to lambda
      When query
        """
        SELECT zip_with(array(1, 2), array(10, 20, 30), (x, y) -> nvl(cast(x AS string), 'was_null')) AS result
        """
      Then query result
        | result                  |
        | [1, 2, was_null]        |

    Scenario: Padded null position in first array is accessible in lambda
      When query
        """
        SELECT zip_with(array(10, 20, 30), array(1, 2), (x, y) -> nvl(cast(y AS string), 'was_null')) AS result
        """
      Then query result
        | result                  |
        | [1, 2, was_null]        |

  Rule: Empty arrays

    Scenario: Both arrays empty - returns empty array
      When query
        """
        SELECT zip_with(array(), array(), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Both typed empty arrays - returns empty array
      When query
        """
        SELECT zip_with(CAST(array() AS ARRAY<INT>), CAST(array() AS ARRAY<INT>), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Empty first array, non-empty second - all second elements become paired with NULL
      When query
        """
        SELECT zip_with(CAST(array() AS ARRAY<INT>), array(1, 2, 3), (x, y) -> x + y) AS result
        """
      Then query result
        | result               |
        | [NULL, NULL, NULL]   |

    Scenario: Non-empty first array, empty second - all first elements become paired with NULL
      When query
        """
        SELECT zip_with(array(1, 2, 3), CAST(array() AS ARRAY<INT>), (x, y) -> x + y) AS result
        """
      Then query result
        | result               |
        | [NULL, NULL, NULL]   |

  Rule: NULL array inputs return NULL

    Scenario: NULL first array returns NULL
      When query
        """
        SELECT zip_with(CAST(NULL AS ARRAY<INT>), array(1, 2, 3), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL second array returns NULL
      When query
        """
        SELECT zip_with(array(1, 2, 3), CAST(NULL AS ARRAY<INT>), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Both NULL arrays return NULL
      When query
        """
        SELECT zip_with(CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL elements within arrays are propagated through the lambda

    Scenario: NULL element in first array propagates through addition
      When query
        """
        SELECT zip_with(array(1, null, 3), array(4, 5, 6), (x, y) -> x + y) AS result
        """
      Then query result
        | result         |
        | [5, NULL, 9]   |

    Scenario: NULL element in second array propagates through addition
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, null, 6), (x, y) -> x + y) AS result
        """
      Then query result
        | result         |
        | [5, NULL, 9]   |

    Scenario: NULL elements in both arrays both propagate
      When query
        """
        SELECT zip_with(array(1, null, 3), array(null, 5, 6), (x, y) -> x + y) AS result
        """
      Then query result
        | result               |
        | [NULL, NULL, 9]      |

    Scenario: NULL element in string array propagates through concat
      When query
        """
        SELECT zip_with(array('a', null, 'c'), array('x', 'y', 'z'), (x, y) -> concat(x, y)) AS result
        """
      Then query result
        | result           |
        | [ax, NULL, cz]   |

  Rule: Type coercion between array element types

    Scenario: Integer and double arrays - elements coerced to double
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(1.5, 2.5, 3.5), (x, y) -> x + y) AS result
        """
      Then query result
        | result           |
        | [2.5, 4.5, 6.5]  |

    Scenario: Long and int arrays - elements coerced to long
      When query
        """
        SELECT zip_with(array(1L, 2L), array(1, 2), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | [2, 4] |

    Scenario: Double arrays with decimal addition
      When query
        """
        SELECT zip_with(array(1.1, 2.2), array(3.3, 4.4), (x, y) -> x + y) AS result
        """
      Then query result
        | result       |
        | [4.4, 6.6]   |

  Rule: Outer column references inside the lambda

    Scenario: Lambda captures a column from the outer query
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> x + y + v) AS result
        FROM (SELECT 10 AS v) t
        """
      Then query result
        | result         |
        | [15, 17, 19]   |

  Rule: Nested arrays as elements

    Scenario: Zip two arrays of arrays, concatenating each pair of inner arrays
      When query
        """
        SELECT zip_with(array(array(1, 2), array(3, 4)), array(array(5, 6), array(7, 8)), (x, y) -> concat(x, y)) AS result
        """
      Then query result
        | result                       |
        | [[1, 2, 5, 6], [3, 4, 7, 8]] |

  Rule: Multi-row queries

    Scenario: zip_with applied to rows with different array lengths
      When query
        """
        SELECT zip_with(a, b, (x, y) -> x + y) AS result
        FROM VALUES
          (array(1, 2), array(3, 4)),
          (array(10, 20, 30), array(1, 2, 3)),
          (array(5), array(6, 7))
        AS t(a, b)
        """
      Then query result
        | result       |
        | [4, 6]       |
        | [11, 22, 33] |
        | [11, NULL]   |

    Scenario: zip_with with NULL array rows returns NULL for those rows
      When query
        """
        SELECT zip_with(col1, col2, (x, y) -> x + y) AS result
        FROM VALUES
          (array(1, 2), array(3, 4)),
          (CAST(NULL AS ARRAY<INT>), array(5, 6)),
          (array(7, 8), CAST(NULL AS ARRAY<INT>))
        AS t(col1, col2)
        """
      Then query result
        | result |
        | [4, 6] |
        | NULL   |
        | NULL   |

    Scenario: zip_with from table with varying-length arrays
      When query
        """
        SELECT zip_with(col1, col2, (x, y) -> x * y) AS result
        FROM VALUES
          (array(1, 2, 3), array(4, 5, 6)),
          (array(10), array(3)),
          (array(2, 4), array(3, 5))
        AS t(col1, col2)
        """
      Then query result
        | result      |
        | [4, 10, 18] |
        | [30]        |
        | [6, 20]     |

  Rule: Composing zip_with with other functions

    Scenario: size() of the result array
      When query
        """
        SELECT size(zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> x + y)) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: Nested zip_with calls
      When query
        """
        SELECT zip_with(
          zip_with(array(1, 2), array(3, 4), (a, b) -> a + b),
          zip_with(array(5, 6), array(7, 8), (a, b) -> a * b),
          (x, y) -> x + y
        ) AS result
        """
      Then query result
        | result    |
        | [39, 54]  |

    Scenario: zip_with on sequence-generated arrays
      When query
        """
        SELECT zip_with(sequence(1, 5), sequence(6, 10), (x, y) -> x + y) AS result
        """
      Then query result
        | result              |
        | [7, 9, 11, 13, 15]  |

  Rule: No aliases exist for zip_with

    Scenario: arrays_zip_with is not a valid function name
      When query
        """
        SELECT arrays_zip_with(array(1, 2), array(3, 4), (x, y) -> x + y) AS result
        """
      Then query error .*

  Rule: Invalid usage produces errors

    Scenario: Too few arguments - 2 instead of 3
      When query
        """
        SELECT zip_with(array(1, 2), array(3, 4)) AS result
        """
      Then query error .*

    Scenario: Too many arguments - 4 instead of 3
      When query
        """
        SELECT zip_with(array(1, 2), array(3, 4), (x, y) -> x + y, 99) AS result
        """
      Then query error .*

    Scenario: First argument is not an array
      When query
        """
        SELECT zip_with(42, array(3, 4), (x, y) -> x + y) AS result
        """
      Then query error .*

    Scenario: Second argument is not an array
      When query
        """
        SELECT zip_with(array(3, 4), 'hello', (x, y) -> x + y) AS result
        """
      Then query error .*

    Scenario: Lambda with only one parameter when two are required
      When query
        """
        SELECT zip_with(array(1, 2), array(3, 4), x -> x) AS result
        """
      Then query error .*

    Scenario: Lambda with three parameters when two are required
      When query
        """
        SELECT zip_with(array(1, 2), array(3, 4), (x, y, z) -> x + y + z) AS result
        """
      Then query error .*
