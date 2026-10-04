Feature: xxhash64() returns 64-bit xxHash

  Rule: Basic usage

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT xxhash64(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                   | args      | result               |
        | xxhash64 integer       | 42        | -387659249110444264  |
        | xxhash64 string        | 'hello'   | -4367754540140381902 |
        | xxhash64 multiple args | 1, 'a', 2 | 4450643625805672383  |

  Rule: Null handling

    Scenario Outline: Null: <case>
      When query
        """
        SELECT xxhash64(<args>) AS result
        """
      Then query result
        | result |
        | 42     |

      Examples:
        | case                                             | args                 |
        | xxhash64 null input                              | CAST(NULL AS INT)    |
        | xxhash64 null string input also returns the seed | CAST(NULL AS STRING) |

  Rule: Type coverage

    # The hash must agree with Spark for every input type, since the encoding fed
    # to xxHash is type-specific. All values verified against the Spark JVM.

    # NOTE: TIMESTAMP (LTZ) is intentionally NOT asserted with a golden value —
    # its hash depends on the session timezone (micros-since-epoch), so the value
    # is not portable across environments. The migration still hashes it; only the
    # golden value would be flaky.
    Scenario Outline: Type: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case             | arg                            | result               |
        | xxhash64 bigint  | CAST(1 AS BIGINT)              | -7001672635703045582 |
        | xxhash64 double  | CAST(1.5 AS DOUBLE)            | 7738255526519901366  |
        | xxhash64 float    | CAST(1.5 AS FLOAT)             | 6163473420726370430  |
        | xxhash64 decimal | CAST(1.50 AS DECIMAL(10,2))    | -6873856301616164681 |
        | xxhash64 date    | DATE '2024-01-15'              | 2166432641145730595  |
        | xxhash64 binary  | X'48656C6C6F'                  | 6777584228807376986  |
        | xxhash64 array   | array(1, 2, 3)                 | 8592097078962733837  |
        | xxhash64 struct  | named_struct('a', 1, 'b', 'x') | 8510603489595372987  |

    Scenario: xxhash64 int and boolean true hash identically
      When query
        """
        SELECT xxhash64(CAST(1 AS INT)) AS i, xxhash64(true) AS b
        """
      Then query result
        | i                    | b                    |
        | -6698625589789238999 | -6698625589789238999 |

  Rule: Boundaries that already agree with Spark

    # XXH64 handles the input in 32-byte stripes, then 8, 4 and 1 byte tails: the string
    # lengths 3/4/7/8/31/32/33/100 walk every one of those branches.
    Scenario Outline: Control: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case           | arg                                        | result               |
        | tinyint        | CAST(1 AS TINYINT)                         | -6698625589789238999 |
        | smallint       | CAST(1 AS SMALLINT)                        | -6698625589789238999 |
        | bool_false     | false                                      | 3614696996920510707  |
        | float_negzero   | CAST('-0.0' AS FLOAT)                      | 3614696996920510707  |
        | double_negzero | CAST('-0.0' AS DOUBLE)                     | -5252525462095825812 |
        | double_poszero | CAST('0.0' AS DOUBLE)                      | -5252525462095825812 |
        | float_nan       | CAST('NaN' AS FLOAT)                       | 2692338816207849720  |
        | double_nan     | CAST('NaN' AS DOUBLE)                      | -3127944061524951246 |
        | double_inf     | CAST('Infinity' AS DOUBLE)                  | 5810986238603807492  |
        | float_inf       | CAST('-Infinity' AS FLOAT)                  | -7580553461823983095 |
        | string_empty   | ''                                         | -7444071767201028348 |
        | string_unicode | '中文😀'                                    | -8063799401152548551 |
        | string_3       | 'abc'                                      | 1423657621850124518  |
        | string_4       | 'abcd'                                     | -6810745876291105281 |
        | string_7       | 'abcdefg'                                  | 3761890393722740389  |
        | string_8       | 'abcdefgh'                                 | 2470326616177429180  |
        | string_31      | repeat('a', 31)                            | 1155894261107880164  |
        | string_32      | repeat('a', 32)                            | 4175410394768388789  |
        | string_33      | repeat('a', 33)                            | 795275648075961603   |
        | string_100     | repeat('ab', 50)                           | 4203777275277848579  |
        | binary_empty   | X''                                        | -7444071767201028348 |
        | binary_32      | unhex(repeat('AB', 32))                    | -5015340879306627761 |
        | date_min       | DATE '0001-01-01'                          | 6773727150484150602  |
        | date_max       | DATE '9999-12-31'                          | 7061898801283288394  |
        | ts             | TIMESTAMP '2024-01-15 12:00:00.123456'     | 5617130697695061134  |
        | ts_ntz         | TIMESTAMP_NTZ '2024-01-15 12:00:00.123456' | 5617130697695061134  |
        | ts_epoch       | TIMESTAMP '1970-01-01 00:00:00'            | -5252525462095825812 |
        | ts_pre_epoch   | TIMESTAMP '1969-12-31 23:59:59.5'          | -6045550564350153773 |
        | dec_18_0       | CAST(1 AS DECIMAL(18,0))                   | -7001672635703045582 |
        | dec_1_0        | CAST(1 AS DECIMAL(1,0))                    | -7001672635703045582 |

    # The same logical value reaches the same hash through different types.
    Scenario Outline: Identity: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | case                           | expr                                                                                                 |
        | tinyint=int                    | xxhash64(CAST(1 AS TINYINT)) = xxhash64(CAST(1 AS INT))                                              |
        | smallint=int                   | xxhash64(CAST(1 AS SMALLINT)) = xxhash64(CAST(1 AS INT))                                             |
        | date=int                       | xxhash64(DATE '1970-01-02') = xxhash64(1)                                                            |
        | timestamp=bigint               | xxhash64(TIMESTAMP '1970-01-01 00:00:00.000001') = xxhash64(CAST(1 AS BIGINT))                       |
        | timestamp_ntz=bigint           | xxhash64(TIMESTAMP_NTZ '1970-01-01 00:00:00.000001') = xxhash64(CAST(1 AS BIGINT))                   |
        | decimal 18 = bigint            | xxhash64(CAST(1 AS DECIMAL(18,0))) = xxhash64(CAST(1 AS BIGINT))                                     |
        | decimal 10,2 = unscaled bigint | xxhash64(CAST(1.5 AS DECIMAL(10,2))) = xxhash64(CAST(150 AS BIGINT))                                 |
        | decimal 19 is not the long     | xxhash64(CAST(1 AS DECIMAL(19,0))) = xxhash64(CAST(1 AS BIGINT))                                     |
        | negative zero double           | xxhash64(CAST('-0.0' AS DOUBLE)) = xxhash64(CAST('0.0' AS DOUBLE))                                   |
        | negative zero float             | xxhash64(CAST('-0.0' AS FLOAT)) = xxhash64(CAST('0.0' AS FLOAT))                                     |
        | string = binary                | xxhash64('abc') = xxhash64(CAST('abc' AS BINARY))                                                    |
        | array folds like arguments     | xxhash64(array(1, 2)) = xxhash64(1, 2)                                                               |
        | struct folds like arguments    | xxhash64(named_struct('a', 1, 'b', 2)) = xxhash64(1, 2)                                              |
        | array skips NULL elements      | xxhash64(array(1, CAST(NULL AS INT), 3)) = xxhash64(1, 3)                                            |
        | empty array is the seed        | xxhash64(CAST(array() AS ARRAY<INT>)) = 42                                                           |
        | nested arrays flatten          | xxhash64(array(array(1, 2), array(3))) = xxhash64(1, 2, 3)                                           |
        | array of decimal 10 folds      | xxhash64(array(CAST(1.5 AS DECIMAL(10,2)))) = xxhash64(CAST(1.5 AS DECIMAL(10,2)))                   |
        | array of timestamp_ntz folds   | xxhash64(array(TIMESTAMP_NTZ '2024-01-15 12:00:00')) = xxhash64(TIMESTAMP_NTZ '2024-01-15 12:00:00') |

    # Collections: NULL elements and NULL fields leave the running hash untouched.
    Scenario Outline: Nested: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                | arg                                                             | result               |
        | arr_empty           | CAST(array() AS ARRAY<INT>)                                     | 42                   |
        | arr_null_elem       | array(1, CAST(NULL AS INT), 3)                                  | -3383274367486667931 |
        | arr_all_null        | array(CAST(NULL AS INT), CAST(NULL AS INT))                     | 42                   |
        | arr_null_arr        | CAST(NULL AS ARRAY<INT>)                                        | 42                   |
        | arr_tinyint         | array(CAST(1 AS TINYINT), CAST(2 AS TINYINT))                   | -8133857028838179022 |
        | arr_smallint        | array(CAST(1 AS SMALLINT), CAST(2 AS SMALLINT))                 | -8133857028838179022 |
        | arr_bigint          | array(CAST(1 AS BIGINT), CAST(2 AS BIGINT))                     | -8198029865082835910 |
        | arr_bool            | array(true, false)                                              | -4248612865510463554 |
        | arr_float            | array(CAST(1.5 AS FLOAT), CAST('-0.0' AS FLOAT))                | -8014894660985922415 |
        | arr_double          | array(CAST(1.5 AS DOUBLE), CAST('-0.0' AS DOUBLE))              | 4084506597711912419  |
        | arr_string          | array('a', 'bb')                                                | -5936128059069371460 |
        | arr_binary          | array(X'01', X'0203')                                           | -3896402547548859816 |
        | arr_date            | array(DATE '2024-01-15')                                        | 2166432641145730595  |
        | arr_ts              | array(TIMESTAMP '2024-01-15 12:00:00')                          | 7962214294659802846  |
        | arr_ts_ntz          | array(TIMESTAMP_NTZ '2024-01-15 12:00:00')                      | 7962214294659802846  |
        | arr_dec10           | array(CAST(1.5 AS DECIMAL(10,2)))                               | -6873856301616164681 |
        | arr_arr             | array(array(1, 2), array(3))                                    | 8592097078962733837  |
        | arr_arr_empty_inner | array(CAST(array() AS ARRAY<INT>), array(3))                    | 6258084186791473711  |
        | arr_arr_arr         | array(array(array(1)))                                          | -6698625589789238999 |
        | arr_struct          | array(named_struct('a', 1, 'b', 'x'))                           | 8510603489595372987  |
        | struct_null_field    | named_struct('a', CAST(NULL AS INT), 'b', 'x')                  | -5636050478767222463 |
        | struct_all_null     | named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS STRING)) | 42                   |
        | struct_null_struct  | CAST(NULL AS STRUCT<a: INT>)                                    | 42                   |
        | struct_nested       | named_struct('a', named_struct('b', 1))                         | -6698625589789238999 |
        | struct_nested_2     | named_struct('a', named_struct('b', named_struct('c', 1)))      | -6698625589789238999 |
        | struct_dec10        | named_struct('a', CAST(1.5 AS DECIMAL(10,2)))                   | -6873856301616164681 |
        | struct_arr          | named_struct('a', array(1, 2))                                  | -8133857028838179022 |
        | struct_ts_ntz       | named_struct('a', TIMESTAMP_NTZ '2024-01-15 12:00:00')          | 7962214294659802846  |

    # The running hash crosses types: the result of one argument is the seed of the next.
    Scenario Outline: Chaining: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case          | arg                                     | result               |
        | int,str       | 1, 'a'                                  | 8205864924878002737  |
        | null,int      | CAST(NULL AS INT), 1                    | -6698625589789238999 |
        | int,null      | 1, CAST(NULL AS INT)                    | -6698625589789238999 |
        | null,null     | CAST(NULL AS INT), CAST(NULL AS STRING) | 42                   |
        | str,arr,int   | 'Spark', array(123), 2                  | 5602566077635097486  |
        | double,double | 1.5D, 2.5D                              | -1870924376432669778 |
        | nulltype arg  | NULL, 1                                 | -6698625589789238999 |



    # Same values through the columnar kernel: distinct values per row plus a NULL row,
    # so a broadcast of the first row or a skipped NULL mask shows up.

    Scenario: column: int
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (1), (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -6698625589789238999 |
        | 8420071140774656230  |
        | 42                   |

    Scenario: column: bigint
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -7001672635703045582 |
        | -3341702809300393011 |
        | 42                   |

    Scenario: column: tinyint
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (CAST(1 AS TINYINT)), (CAST(-2 AS TINYINT)), (CAST(NULL AS TINYINT)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -6698625589789238999 |
        | 6162728026222640212  |
        | 42                   |

    Scenario: column: smallint
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (CAST(1 AS SMALLINT)), (CAST(-2 AS SMALLINT)), (CAST(NULL AS SMALLINT)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -6698625589789238999 |
        | 6162728026222640212  |
        | 42                   |

    Scenario: column: bool
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (true), (false), (CAST(NULL AS BOOLEAN)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -6698625589789238999 |
        | 3614696996920510707  |
        | 42                   |

    Scenario: column: float
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (CAST(1.5 AS FLOAT)), (CAST('-0.0' AS FLOAT)), (CAST(NULL AS FLOAT)) AS t(c)
        """
      Then query result ordered
        | result              |
        | 6163473420726370430 |
        | 3614696996920510707 |
        | 42                  |

    Scenario: column: double
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (1.5D), (CAST('-0.0' AS DOUBLE)), (CAST(NULL AS DOUBLE)) AS t(c)
        """
      Then query result ordered
        | result               |
        | 7738255526519901366  |
        | -5252525462095825812 |
        | 42                   |

    Scenario: column: string
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES ('a'), (''), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -8582455328737087284 |
        | -7444071767201028348 |
        | 42                   |

    Scenario: column: string_long
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (repeat('x', 33)), ('中文'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -2751654916054470392 |
        | -7082105647848352025 |
        | 42                   |

    Scenario: column: binary
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (X'01'), (X''), (CAST(NULL AS BINARY)) AS t(c)
        """
      Then query result ordered
        | result               |
        | 6668291691252061002  |
        | -7444071767201028348 |
        | 42                   |

    Scenario: column: date
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (DATE '2024-01-15'), (DATE '1969-12-31'), (CAST(NULL AS DATE)) AS t(c)
        """
      Then query result ordered
        | result              |
        | 2166432641145730595 |
        | 2017008487422258757 |
        | 42                  |

    Scenario: column: ts
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (TIMESTAMP '2024-01-15 12:00:00.123456'), (TIMESTAMP '1969-12-31 23:59:59.5'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query result ordered
        | result               |
        | 5617130697695061134  |
        | -6045550564350153773 |
        | 42                   |

    Scenario: column: ts_ntz
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (TIMESTAMP_NTZ '2024-01-15 12:00:00.123456'), (TIMESTAMP_NTZ '1969-12-31 23:59:59.5'), (CAST(NULL AS TIMESTAMP_NTZ)) AS t(c)
        """
      Then query result ordered
        | result               |
        | 5617130697695061134  |
        | -6045550564350153773 |
        | 42                   |

    Scenario: column: dec10
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (CAST(1.5 AS DECIMAL(10,2))), (CAST(-1.5 AS DECIMAL(10,2))), (CAST(NULL AS DECIMAL(10,2))) AS t(c)
        """
      Then query result ordered
        | result               |
        | -6873856301616164681 |
        | -3700955262752989823 |
        | 42                   |

    Scenario: column: dec18
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (CAST(1 AS DECIMAL(18,0))), (CAST(-1 AS DECIMAL(18,0))), (CAST(NULL AS DECIMAL(18,0))) AS t(c)
        """
      Then query result ordered
        | result               |
        | -7001672635703045582 |
        | 3858142552250413010  |
        | 42                   |

    Scenario: column: arr_int
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (array(1, 2)), (array(3)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -8133857028838179022 |
        | 6258084186791473711  |
        | 42                   |

    Scenario: column: arr_str
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (array('a', 'b')), (array('c')), (CAST(NULL AS ARRAY<STRING>)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -7357372313250070212 |
        | -3446348257159517350 |
        | 42                   |

    Scenario: column: arr_arr
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (array(array(1, 2), array(3))), (array(array(4))), (CAST(NULL AS ARRAY<ARRAY<INT>>)) AS t(c)
        """
      Then query result ordered
        | result               |
        | 8592097078962733837  |
        | -8344648708406692296 |
        | 42                   |

    Scenario: column: struct
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (named_struct('a', 1, 'b', 'x')), (named_struct('a', 2, 'b', 'y')), (CAST(NULL AS STRUCT<a: INT, b: STRING>)) AS t(c)
        """
      Then query result ordered
        | result               |
        | 8510603489595372987  |
        | -2550185233032882440 |
        | 42                   |

  Rule: Decimals wider than 18 digits

    # Spark 4.2.0 hash.scala (InterpretedHashFunction, `case d: Decimal`): precision <= 18
    # hashes the unscaled long; above that it hashes the BigInteger.toByteArray bytes
    # (big-endian, minimal length, two's complement).
    # 128 and -128 sit on the boundary where the minimal encoding grows to 2 bytes.
    Scenario Outline: Wide decimal: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case          | arg                                                              | result               |
        | dec_19_0      | CAST(1 AS DECIMAL(19,0))                                         | 6668291691252061002  |
        | dec_20_0      | CAST(1 AS DECIMAL(20,0))                                         | 6668291691252061002  |
        | dec_30_2      | CAST(1 AS DECIMAL(30,2))                                         | 4409375254388155230  |
        | dec_38_0_max  | CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))  | -47190729175993179   |
        | dec_38_0_neg  | CAST('-99999999999999999999999999999999999999' AS DECIMAL(38,0)) | -2254039905620870768 |
        | dec_30_neg1   | CAST(-1 AS DECIMAL(30,0))                                        | -4006032525457443936 |
        | dec_30_128    | CAST(128 AS DECIMAL(30,0))                                       | 6715097930120473301  |
        | dec_30_neg128 | CAST(-128 AS DECIMAL(30,0))                                      | -7898411661632158123 |
        | dec_30_256    | CAST(256 AS DECIMAL(30,0))                                       | 4805153208743283185  |
        | dec_30_zero   | CAST(0 AS DECIMAL(30,0))                                         | -8959994473701255385 |
        | dec_38_37     | CAST(0.5 AS DECIMAL(38,37))                                      | 4769498902473377579  |

    Scenario Outline: Identity: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | case                 | expr                                                     |
        | 19 digits = bytes 01 | xxhash64(CAST(1 AS DECIMAL(19,0))) = xxhash64(X'01')     |
        | 128 = bytes 0080     | xxhash64(CAST(128 AS DECIMAL(30,0))) = xxhash64(X'0080') |
        | -1 = bytes FF        | xxhash64(CAST(-1 AS DECIMAL(30,0))) = xxhash64(X'FF')    |

    Scenario Outline: Wide decimal inside: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case             | arg                                                                                                         | result              |
        | arr_dec30        | array(CAST(1.5 AS DECIMAL(30,2)))                                                                           | -194698251525976593 |
        | struct_dec30     | named_struct('a', CAST(1.5 AS DECIMAL(30,2)))                                                               | -194698251525976593 |
        | dec30 and int    | CAST(1 AS DECIMAL(30,2)), 1                                                                                 | 1282602492964177816 |
        | 10 mixed         | 1, 'a', 2.5D, true, DATE '2024-01-15', X'01', array(1), named_struct('a',1), CAST(1 AS DECIMAL(20,0)), NULL | 9027594118702890900 |
        | between decimals | CAST(1 AS DECIMAL(18,0)), CAST(1 AS DECIMAL(19,0))                                                          | 3428384918159051943 |

    Scenario: column: decimal 19
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (CAST(1 AS DECIMAL(19,0))), (CAST(-1 AS DECIMAL(19,0))), (CAST(NULL AS DECIMAL(19,0))) AS t(c)
        """
      Then query result ordered
        | result               |
        | 6668291691252061002  |
        | -4006032525457443936 |
        | 42                   |

    Scenario: column: decimal 30
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (CAST(1 AS DECIMAL(30,2))), (CAST(-128 AS DECIMAL(30,2))), (CAST(NULL AS DECIMAL(30,2))) AS t(c)
        """
      Then query result ordered
        | result               |
        | 4409375254388155230  |
        | -2820309155850358339 |
        | 42                   |

    Scenario: column: decimal 38
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))), (CAST('-1' AS DECIMAL(38,0))), (CAST(NULL AS DECIMAL(38,0))) AS t(c)
        """
      Then query result ordered
        | result               |
        | -47190729175993179   |
        | -4006032525457443936 |
        | 42                   |

    Scenario: column: array of decimal 30
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (array(CAST(1 AS DECIMAL(30,2)))), (array(CAST(-128 AS DECIMAL(30,2)))), (CAST(NULL AS ARRAY<DECIMAL(30,2)>)) AS t(c)
        """
      Then query result ordered
        | result               |
        | 4409375254388155230  |
        | -2820309155850358339 |
        | 42                   |

    Scenario: column: struct of decimal 30
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (named_struct('a', CAST(1 AS DECIMAL(30,2)))), (named_struct('a', CAST(-128 AS DECIMAL(30,2)))), (CAST(NULL AS STRUCT<a: DECIMAL(30,2)>)) AS t(c)
        """
      Then query result ordered
        | result               |
        | 4409375254388155230  |
        | -2820309155850358339 |
        | 42                   |

    Scenario: column: decimal 30 as one of three arguments
      When query
        """
        SELECT xxhash64(a, b, d) AS result FROM VALUES (1, 'a', CAST(5 AS DECIMAL(30,0))), (2, 'b', CAST(-5 AS DECIMAL(30,0))) AS t(a, b, d)
        """
      Then query result ordered
        | result               |
        | -8070725420776589759 |
        | 7357274855316755055  |

  Rule: Interval types

    # Spark hashes YEAR-MONTH as an int, DAY-TIME as a long and the calendar interval as
    # (months, days, microseconds) chained from the microseconds.
    # The days-only and micros-only rows tell the calendar interval fields apart.
    Scenario Outline: Interval: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | arg                                   | result               |
        | ym_interval              | INTERVAL '1-2' YEAR TO MONTH          | -3715488255824917081 |
        | ym_interval_year         | INTERVAL '3' YEAR                     | 7419680032772472637  |
        | ym_interval_month        | INTERVAL '3' MONTH                    | 6258084186791473711  |
        | dt_interval              | INTERVAL '1 02:03:04.5' DAY TO SECOND | -5867921652851782348 |
        | dt_interval_day          | INTERVAL '5' DAY                      | 5930000196743745572  |
        | dt_interval_hour         | INTERVAL '5' HOUR                     | -4046135109051420118 |
        | cal_interval             | make_interval(1, 2, 3, 4, 5, 6, 7.5)  | 6449704631820846915  |
        | cal_interval_months_only | make_interval(1, 2)                   | -1828810999893585240 |
        | cal_interval_days_only   | make_interval(0, 0, 0, 4)             | -1596980011289455982 |
        | cal_interval_micros_only | make_interval(0, 0, 0, 0, 0, 0, 7.5)  | -4815102596505235544 |

    Scenario Outline: Interval inside: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case       | arg                                                    | result               |
        | arr_ym     | array(INTERVAL '1-2' YEAR TO MONTH)                    | -3715488255824917081 |
        | arr_dt     | array(INTERVAL '1 02:03:04' DAY TO SECOND)             | 4873107316074688783  |
        | arr_cal    | array(make_interval(1, 2, 3, 4))                       | -2276548691635096493 |
        | struct_ym  | named_struct('a', INTERVAL '1-2' YEAR TO MONTH)        | -3715488255824917081 |
        | struct_dt  | named_struct('a', INTERVAL '1 02:03:04' DAY TO SECOND) | 4873107316074688783  |
        | struct_cal | named_struct('a', make_interval(1, 2, 3, 4))           | -2276548691635096493 |

    Scenario Outline: Identity: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | case                      | expr                                                                                                 |
        | year-month = int          | xxhash64(INTERVAL '1' MONTH) = xxhash64(CAST(1 AS INT))                                              |
        | day-time = bigint         | xxhash64(INTERVAL '0 00:00:00.000001' DAY TO SECOND) = xxhash64(CAST(1 AS BIGINT))                   |
        | array of year-month folds | xxhash64(array(INTERVAL '1-2' YEAR TO MONTH)) = xxhash64(INTERVAL '1-2' YEAR TO MONTH)               |
        | array of day-time folds   | xxhash64(array(INTERVAL '1 02:03:04' DAY TO SECOND)) = xxhash64(INTERVAL '1 02:03:04' DAY TO SECOND) |
        | array of calendar folds   | xxhash64(array(make_interval(1, 2, 3, 4))) = xxhash64(make_interval(1, 2, 3, 4))                     |

    Scenario: mixed with other types in one call
      When query
        """
        SELECT xxhash64(TIMESTAMP '2024-01-15 12:00:00', DATE '2024-01-15', make_interval(1,2,3,4)) AS result
        """
      Then query result
        | result               |
        | -7817275418368096527 |

    Scenario: column: year-month
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (INTERVAL '1-2' YEAR TO MONTH), (INTERVAL '-3' MONTH), (CAST(NULL AS INTERVAL YEAR TO MONTH)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -3715488255824917081 |
        | 1399955818750051488  |
        | 42                   |

    Scenario: column: day-time
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (INTERVAL '1 02:03:04.5' DAY TO SECOND), (INTERVAL '-5' DAY), (CAST(NULL AS INTERVAL DAY TO SECOND)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -5867921652851782348 |
        | -3287489565778354819 |
        | 42                   |

    Scenario: column: calendar interval
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (make_interval(1,2,3,4)), (make_interval(0,0,0,0,0,0,7.5)), (CAST(NULL AS INTERVAL)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -2276548691635096493 |
        | -4815102596505235544 |
        | 42                   |

    Scenario: column: struct of calendar interval
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (named_struct('a', make_interval(1,2,3,4))), (named_struct('a', make_interval(0,0,0,0,0,0,7.5))), (CAST(NULL AS STRUCT<a: INTERVAL>)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -2276548691635096493 |
        | -4815102596505235544 |
        | 42                   |

  Rule: TIME type

    # Spark hashes TIME as a long (nanoseconds of day). TIME literals need Spark 4.1+.
    @spark-4.1
    Scenario Outline: Time: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case   | arg                    | result               |
        | time   | TIME '12:30:00.123456' | 7635627404547864269  |
        | time_0 | TIME '00:00:00'        | -5252525462095825812 |

    @spark-4.1
    Scenario: Time: TIME hashes like its nanoseconds as a long
      When query
        """
        SELECT xxhash64(TIME '00:00:00.000001') = xxhash64(CAST(1000 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | true   |

    @spark-4.1
    Scenario Outline: Time inside: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case        | arg                                | result              |
        | arr_time    | array(TIME '12:30:00')             | 1128842886192098135 |
        | struct_time | named_struct('a', TIME '12:30:00') | 1128842886192098135 |

  Rule: Arguments Spark refuses at analysis time

    # hash.scala checkInputDataTypes: no arguments -> WRONG_NUM_ARGS; any MAP (also nested) ->
    # HASH_MAP_TYPE unless spark.sql.legacy.allowHashOnMapType; any VARIANT -> HASH_VARIANT_TYPE.
    Scenario: no arguments
      When query
        """
        SELECT xxhash64()
        """
      Then query error requires > 0 parameters

    Scenario Outline: map: <case>
      When query
        """
        SELECT xxhash64(<arg>)
        """
      Then query error cannot contain elements of the "MAP" type

      Examples:
        | case                       | arg                            |
        | map                        | map('a', 1)                    |
        | empty map                  | CAST(map() AS MAP<STRING,INT>) |
        | map in array               | array(map('a', 1))             |
        | map in struct              | named_struct('a', map('a', 1)) |
        | map after a valid argument | 1, map('a', 1)                 |

    Scenario Outline: variant: <case>
      When query
        """
        SELECT xxhash64(<arg>)
        """
      Then query error cannot contain elements of the "VARIANT" type

      Examples:
        | case              | arg                                |
        | variant           | parse_json('1')                    |
        | variant in array  | array(parse_json('1'))             |
        | variant in struct | named_struct('a', parse_json('1')) |

    # With the legacy flag Spark hashes the map as key, value pairs in order.
    Scenario Outline: legacy map hashing: <case>
      Given config spark.sql.legacy.allowHashOnMapType = true
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | arg                            | result               |
        | string to int         | map('a', 1)                    | -4810547614043336770 |
        | int to string         | map(1, 'a')                    | 8205864924878002737  |
        | int to int            | map(1, 2)                      | -8133857028838179022 |
        | empty map is the seed | CAST(map() AS MAP<STRING,INT>) | 42                   |
        | map in array          | array(map('a', 1))             | -4810547614043336770 |
        | map in struct         | named_struct('a', map('a', 1)) | -4810547614043336770 |

  Rule: Extreme values

    # Type limits, subnormals, the 18/19 digit decimal boundary, long strings, NUL and combining
    # characters, and nesting depth. The hash must agree with Spark for every one of them.
    Scenario Outline: Extreme: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | arg                                                                           | result               |
        | int min                     | CAST(-2147483648 AS INT)                                                      | 2073849959933241805  |
        | int max                     | CAST(2147483647 AS INT)                                                       | 1508894993788531228  |
        | long min                    | CAST(-9223372036854775808 AS BIGINT)                                          | -8619748838626508300 |
        | long max                    | CAST(9223372036854775807 AS BIGINT)                                           | -3246596055638297850 |
        | tinyint min                 | CAST(-128 AS TINYINT)                                                         | 4160238337661960656  |
        | tinyint max                 | CAST(127 AS TINYINT)                                                          | 8632298611707923906  |
        | smallint min                | CAST(-32768 AS SMALLINT)                                                      | -904511417458573795  |
        | smallint max                | CAST(32767 AS SMALLINT)                                                       | 8952525448871805501  |
        | float max                    | CAST('3.4028235E38' AS FLOAT)                                                 | -1065250890878313112 |
        | float min subnormal          | CAST('1.4E-45' AS FLOAT)                                                      | -6698625589789238999 |
        | float neg max                | CAST('-3.4028235E38' AS FLOAT)                                                | -8545425418825163117 |
        | double max                  | CAST('1.7976931348623157E308' AS DOUBLE)                                      | -4222314252576420879 |
        | double min subnormal        | CAST('4.9E-324' AS DOUBLE)                                                    | -7001672635703045582 |
        | double neg inf              | CAST('-Infinity' AS DOUBLE)                                                    | 5326262080505358431  |
        | double one third            | CAST(1 AS DOUBLE) / 3                                                         | -6490652648967164210 |
        | dec 18 max                  | CAST('999999999999999999' AS DECIMAL(18,0))                                   | 2162198894918931945  |
        | dec 18 min                  | CAST('-999999999999999999' AS DECIMAL(18,0))                                  | 4265531446127695490  |
        | dec 19 smallest             | CAST('1000000000000000000' AS DECIMAL(19,0))                                  | -1012199175776213219 |
        | dec 38 38                   | CAST('0.99999999999999999999999999999999999999' AS DECIMAL(38,38))            | -47190729175993179   |
        | dec 38 neg one scale10      | CAST('-1.0000000000' AS DECIMAL(38,10))                                       | 8632574043661361134  |
        | dec long overflow unscaled  | CAST('9223372036854775808' AS DECIMAL(19,0))                                   | 2657851292443253618  |
        | dec long underflow unscaled | CAST('-9223372036854775809' AS DECIMAL(19,0))                                  | 3800235373674817761  |
        | string 10k                  | repeat('x', 10000)                                                            | 4067659272742946101  |
        | string nul byte             | concat('a', char(0), 'b')                                                     | -7028150433230623758 |
        | string combining            | CAST(unhex('65CC81') AS STRING)                                               | -2923010410306571269 |
        | string emoji                | '😀'                                                                          | -5869505314936196641 |
        | string whitespace           | '   '                                                                         | 2590154795924065095  |
        | binary nul                  | unhex('00')                                                                   | -8959994473701255385 |
        | binary ff                   | unhex('FFFFFFFF')                                                             | 2017008487422258757  |
        | date epoch                  | DATE '1970-01-01'                                                             | 3614696996920510707  |
        | date pre epoch              | DATE '1969-12-31'                                                             | 2017008487422258757  |
        | ts max                      | TIMESTAMP '9999-12-31 23:59:59.999999'                                        | -3238974501849525030 |
        | ts min                      | TIMESTAMP '0001-01-01 00:00:00'                                               | 4148024589635468106  |
        | arr 100                     | sequence(1, 100)                                                              | 6801588422952230000  |
        | arr deep                    | array(array(array(array(1))))                                                 | -6698625589789238999 |
        | struct deep                 | named_struct('a', named_struct('b', named_struct('c', named_struct('d', 1)))) | -6698625589789238999 |
        | arr struct arr              | array(named_struct('a', array(1, 2)), named_struct('a', array(3)))            | 8592097078962733837  |
        | arr of null arrays          | array(CAST(NULL AS ARRAY<INT>), array(1))                                     | -6698625589789238999 |
        | struct of empty array       | named_struct('a', CAST(array() AS ARRAY<INT>), 'b', 1)                        | -6698625589789238999 |
        | arr empty strings           | array('', '')                                                                 | 6372822119346140258  |

  Rule: Foldable and non-foldable arguments

    # Spark folds a constant argument at planning time and evaluates a column per row. Both ways
    # must reach the same hash.
    Scenario Outline: Foldable: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | expr                                                            | result |
        | 1+1 = 2               | xxhash64(1 + 1) = xxhash64(2)                                   | true   |
        | upper('a') = 'A'      | xxhash64(upper('a')) = xxhash64('A')                            | true   |
        | if(true) picks branch | xxhash64(IF(true, 1, 2)) = xxhash64(1)                          | true   |
        | case foldable         | xxhash64(CASE WHEN 1 = 1 THEN 'x' ELSE 'y' END) = xxhash64('x') | true   |
        | concat foldable       | xxhash64(concat('a', 'b')) = xxhash64('ab')                     | true   |
        | cast foldable         | xxhash64(CAST(1 AS BIGINT)) = xxhash64(1L)                      | true   |
        | arg order matters     | xxhash64(1, 2) = xxhash64(2, 1)                                 | false  |
        | subquery arg          | xxhash64((SELECT 5)) = xxhash64(5)                              | true   |

    Scenario: Column: column equals literal
      When query
        """
        SELECT xxhash64(c) = xxhash64(5) AS result FROM VALUES (5), (6), (CAST(NULL AS INT)) AS t(c)
        """
      Then query result ordered
        | result |
        | true   |
        | false  |
        | false  |

    Scenario: Column: range id vs literal
      When query
        """
        SELECT xxhash64(id) = xxhash64(CAST(2 AS BIGINT)) AS result FROM range(4)
        """
      Then query result ordered
        | result |
        | false  |
        | false  |
        | true   |
        | false  |

    Scenario: Column: column string vs literal
      When query
        """
        SELECT xxhash64(c) = xxhash64('b') AS result FROM VALUES ('a'), ('b') AS t(c)
        """
      Then query result ordered
        | result |
        | false  |
        | true   |

    Scenario: Column: column decimal30 vs literal
      When query
        """
        SELECT xxhash64(c) = xxhash64(CAST(7 AS DECIMAL(30,0))) AS result FROM VALUES (CAST(7 AS DECIMAL(30,0))), (CAST(8 AS DECIMAL(30,0))) AS t(c)
        """
      Then query result ordered
        | result |
        | true   |
        | false  |

    Scenario: Column: mixed literal and column
      When query
        """
        SELECT xxhash64(1, c, 'z') = xxhash64(1, 'a', 'z') AS result FROM VALUES ('a'), ('b') AS t(c)
        """
      Then query result ordered
        | result |
        | true   |
        | false  |

    Scenario: Column: distinct per row
      When query
        """
        SELECT count(DISTINCT xxhash64(id)) AS result FROM range(100)
        """
      Then query result ordered
        | result |
        | 100    |

    Scenario: A query spread over several lines
      When query
        """
        SELECT xxhash64(
          1,
          'a',
          2
        ) AS result
        """
      Then query result
        | result              |
        | 4450643625805672383 |

  Rule: ANSI mode off

    # xxhash64 never reads spark.sql.ansi.enabled. Its argument can: with ANSI off a failing
    # division or cast yields NULL, and the hash of NULL is the seed 42.
    Scenario Outline: ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                      | expr                                      | result               |
        | array of bad cast         | xxhash64(array(CAST('abc' AS INT), 1))    | -6698625589789238999 |
        | bad cast to date          | xxhash64(CAST('not a date' AS DATE))      | 42                   |
        | bad cast to int           | xxhash64(CAST('abc' AS INT))              | 42                   |
        | decimal 30 value          | xxhash64(CAST(1 AS DECIMAL(30,2)))        | 4409375254388155230  |
        | decimal cast overflow      | xxhash64(CAST(12345.678 AS DECIMAL(5,2))) | 42                   |
        | divide by zero            | xxhash64(1 / 0)                           | 42                   |
        | divide by zero second arg | xxhash64(7, 1 / 0)                        | 2786828215451145335  |
        | double nan                | xxhash64(CAST('NaN' AS DOUBLE))           | -3127944061524951246 |
        | int overflow               | xxhash64(2147483647 + 1)                  | 2073849959933241805  |
        | string value              | xxhash64('hello')                         | -4367754540140381902 |
        | try_cast bad              | xxhash64(try_cast('abc' AS INT))          | 42                   |

    Scenario: ANSI off: column column divide by zero
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT xxhash64(10 / c) AS result FROM VALUES (1), (0) AS t(c)
        """
      Then query result ordered
        | result               |
        | -7537455386021189235 |
        | 42                   |

  Rule: ANSI mode on

    # xxhash64 never reads spark.sql.ansi.enabled. Its argument can: with ANSI off a failing
    # division or cast yields NULL, and the hash of NULL is the seed 42.
    Scenario Outline: ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case             | expr                               | result               |
        | decimal 30 value | xxhash64(CAST(1 AS DECIMAL(30,2))) | 4409375254388155230  |
        | double nan       | xxhash64(CAST('NaN' AS DOUBLE))    | -3127944061524951246 |
        | string value     | xxhash64('hello')                  | -4367754540140381902 |
        | try_cast bad     | xxhash64(try_cast('abc' AS INT))   | 42                   |

  Rule: Collated strings

    # Default: spark.sql.legacy.collationAwareHashFunctions = false, so a collated string hashes its raw bytes.
    # Spark 4.2.0 hash.scala: XxHash64 is not collation aware, so a collated string hashes its raw
    # UTF-8 bytes (UTF8_LCASE 'Hello' hashes like plain 'Hello'). Only with the legacy flag
    # spark.sql.legacy.collationAwareHashFunctions does it hash the collation key instead.
    # TODO(spark.sql.legacy.collationAwareHashFunctions): none of the scenarios of this Rule can run yet,
    # because Sail rejects COLLATE in the SQL parser, `collate`/`collation` are unimplemented, and the
    # collation of a string type is dropped by Spark Connect (proto/data_type.rs), spec::DataType and the
    # resolver. xxhash64 itself already hashes the raw bytes, which is Spark's default, so the 10 `Default`
    # scenarios need no change in the kernel. The work belongs to a separate collations PR, in this order:
    #   1. parse `expr COLLATE name` and `STRING COLLATE name`;
    #   2. carry the collation in the type (spec, Connect in/out, field metadata like the interval one);
    #   3. reject, with UNSUPPORTED_COLLATION, every collation-aware operator over a collation other than
    #      UTF8_BINARY. Parsing without this step would silently change results ('a' COLLATE UTF8_LCASE = 'A'
    #      is true in Spark);
    #   4. only then the legacy flag here: read it from PlanConfig as allowHashOnMapType is read, and hash the
    #      collation key (LCASE and RTRIM are pure Rust, UNICODE_CI needs ICU parity with Java).
    @spark-4
    @sail-bug
    Scenario Outline: Default: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                       | arg                                 | result              |
        | UTF8_BINARY                | 'Hello' COLLATE UTF8_BINARY         | 6777584228807376986 |
        | UTF8_LCASE keeps the bytes | 'Hello' COLLATE UTF8_LCASE          | 6777584228807376986 |
        | UTF8_LCASE upper case      | 'HELLO' COLLATE UTF8_LCASE          | 7222086370645328306 |
        | UNICODE_CI keeps the bytes | 'Hello' COLLATE UNICODE_CI          | 6777584228807376986 |
        | UTF8_BINARY_RTRIM keeps it | 'Hello  ' COLLATE UTF8_BINARY_RTRIM | -518140796973092428 |

    @spark-4
    @sail-bug
    Scenario Outline: Default identity: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | expr                                                                        | result |
        | UTF8_LCASE is the plain string    | xxhash64('Hello' COLLATE UTF8_LCASE) = xxhash64('Hello')                    | true   |
        | UTF8_LCASE does not fold the case | xxhash64('Hello' COLLATE UTF8_LCASE) = xxhash64('HELLO' COLLATE UTF8_LCASE) | false  |
        | UNICODE_CI does not fold the case | xxhash64('Hello' COLLATE UNICODE_CI) = xxhash64('HELLO' COLLATE UNICODE_CI) | false  |
        | RTRIM does not trim               | xxhash64('Hello  ' COLLATE UTF8_BINARY_RTRIM) = xxhash64('Hello')           | false  |

    @spark-4
    @sail-bug
    Scenario: Default: a collated column hashes the raw bytes of each row
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES ('Hello' COLLATE UTF8_LCASE), ('HELLO' COLLATE UTF8_LCASE) AS t(c)
        """
      Then query result ordered
        | result              |
        | 6777584228807376986 |
        | 7222086370645328306 |

    @spark-4
    @sail-bug
    Scenario Outline: Legacy collation aware hashing: <case>
      Given config spark.sql.legacy.collationAwareHashFunctions = true
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case              | arg                                 | result               |
        | UTF8_LCASE        | 'Hello' COLLATE UTF8_LCASE          | -6107630433723194353 |
        | UNICODE_CI        | 'Hello' COLLATE UNICODE_CI          | -8477994195443743051 |
        | UTF8_BINARY_RTRIM | 'Hello  ' COLLATE UTF8_BINARY_RTRIM | 9172617197352443106  |

    @spark-4
    @sail-bug
    Scenario Outline: Legacy collation aware hashing identity: <case>
      Given config spark.sql.legacy.collationAwareHashFunctions = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | expr                                                                                        | result |
        | UTF8_LCASE folds the case       | xxhash64('Hello' COLLATE UTF8_LCASE) = xxhash64('HELLO' COLLATE UTF8_LCASE)                 | true   |
        | UNICODE_CI folds the case       | xxhash64('Hello' COLLATE UNICODE_CI) = xxhash64('HELLO' COLLATE UNICODE_CI)                 | true   |
        | RTRIM trims the trailing spaces | xxhash64('Hello  ' COLLATE UTF8_BINARY_RTRIM) = xxhash64('Hello' COLLATE UTF8_BINARY_RTRIM) | true   |

    # A string without collation is not affected by the legacy flag.
    @spark-4
    Scenario: Legacy flag leaves a plain string alone
      Given config spark.sql.legacy.collationAwareHashFunctions = true
      When query
        """
        SELECT xxhash64('Hello') AS result
        """
      Then query result
        | result              |
        | 6777584228807376986 |

  Rule: Map arguments with spark.sql.legacy.allowHashOnMapType

    # Default: spark.sql.legacy.allowHashOnMapType = false, so a MAP argument is rejected (see the Rule about
    # arguments Spark refuses). Every scenario below turns the flag on explicitly.
    # Spark 4.2.0 hash.scala (InterpretedHashFunction, `case map: MapData`): each entry hashes the key
    # and then the value, in insertion order, with the running hash as seed. The entry order matters.
    Scenario Outline: Map: <case>
      Given config spark.sql.legacy.allowHashOnMapType = true
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                    | arg                                                     | result               |
        | two entries             | map('a', 1, 'b', 2)                                     | 7428138486932782616  |
        | two entries reversed    | map('b', 2, 'a', 1)                                     | -3571583358807004113 |
        | null value              | map('a', CAST(NULL AS INT))                             | -8582455328737087284 |
        | null value among others | map('a', 1, 'b', CAST(NULL AS INT), 'c', 3)             | -6308092526048359203 |
        | int keys                | map(1, 'a', 2, 'b')                                     | 8744364272684979380  |
        | array values            | map('a', array(1, 2), 'b', array(3))                    | -2966795663310284637 |
        | struct values           | map('a', named_struct('x', 1, 'y', 'z'))                | -1468509731185257751 |
        | map values              | map('a', map('b', 1))                                   | 8179590835503667989  |
        | decimal 30 values       | map('a', CAST(1 AS DECIMAL(30,2)))                      | -4731006560505675411 |
        | double keys             | map(CAST(1.5 AS DOUBLE), 'a')                           | 647047389824316451   |
        | empty                   | CAST(map() AS MAP<STRING,INT>)                          | 42                   |
        | map_from_arrays         | map_from_arrays(array('a', 'b'), array(1, 2))           | 7428138486932782616  |
        | map_from_entries        | map_from_entries(array(struct('a', 1), struct('b', 2))) | 7428138486932782616  |
        | map_concat              | map_concat(map('a', 1), map('b', 2))                    | 7428138486932782616  |
        | second argument         | 1, map('a', 1)                                          | -1771785051822675751 |
        | two maps                | map('a', 1), map('b', 2)                                | 7428138486932782616  |
        | array of maps           | array(map('a', 1), map('b', 2))                         | 7428138486932782616  |
        | struct with map         | named_struct('m', map('a', 1), 'n', 5)                  | 5792825584738048016  |

    Scenario Outline: Map identity: <case>
      Given config spark.sql.legacy.allowHashOnMapType = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                       | expr                                                                                    | result |
        | insertion order equals     | xxhash64(map('a', 1, 'b', 2)) = xxhash64(map('b', 2, 'a', 1))                           | false  |
        | from arrays equals literal | xxhash64(map_from_arrays(array('a', 'b'), array(1, 2))) = xxhash64(map('a', 1, 'b', 2)) | true   |

    Scenario: Map: column
      Given config spark.sql.legacy.allowHashOnMapType = true
      When query
        """
        SELECT xxhash64(m) AS result FROM VALUES (map('a', 1)), (map('b', 2)), (CAST(NULL AS MAP<STRING,INT>)) AS t(m)
        """
      Then query result ordered
        | result               |
        | -4810547614043336770 |
        | 9142465394519884428  |
        | 42                   |

    Scenario: Map: column two entries
      Given config spark.sql.legacy.allowHashOnMapType = true
      When query
        """
        SELECT xxhash64(m) AS result FROM VALUES (map('a', 1, 'b', 2)), (map('b', 2, 'a', 1)) AS t(m)
        """
      Then query result ordered
        | result               |
        | 7428138486932782616  |
        | -3571583358807004113 |

    Scenario: Map: rejected again after the flag
      Given config spark.sql.legacy.allowHashOnMapType = false
      When query
        """
        SELECT xxhash64(map('a', 1)) AS result
        """
      Then query error cannot contain elements of the "MAP" type

    Scenario: Map: duplicate key
      Given config spark.sql.legacy.allowHashOnMapType = true
      When query
        """
        SELECT xxhash64(map('a', 1, 'a', 2)) AS result
        """
      Then query error Duplicate map key a was found

    Scenario: Dedup policy LAST_WIN: duplicate key last win
      Given config spark.sql.legacy.allowHashOnMapType = true
      And config spark.sql.mapKeyDedupPolicy = LAST_WIN
      When query
        """
        SELECT xxhash64(map('a', 1, 'a', 2)) AS result
        """
      Then query result
        | result               |
        | -8209314916407280043 |

    Scenario: Dedup policy LAST_WIN: duplicate key last win equals single
      Given config spark.sql.legacy.allowHashOnMapType = true
      And config spark.sql.mapKeyDedupPolicy = LAST_WIN
      When query
        """
        SELECT xxhash64(map('a', 1, 'a', 2)) = xxhash64(map('a', 2)) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Session time zone

    # Default: the test harness runs the session in UTC (spark.sql.session.timeZone = UTC), and a scenario
    # without `Given config` is measured in UTC. Spark itself would use the JVM zone.
    # The kernel never reads spark.sql.session.timeZone, but a TIMESTAMP is stored as UTC micros, so the
    # zone decides the value that is hashed. TIMESTAMP_NTZ and DATE ignore it.
    Scenario: Time zone: the default is UTC
      When query
        """
        SELECT xxhash64(TIMESTAMP '2024-01-15 12:00:00') AS result
        """
      Then query result
        | result              |
        | 7962214294659802846 |

    Scenario Outline: Time zone: <case> in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                       | zone                | arg                                                | result               |
        | timestamp                  | UTC                 | TIMESTAMP '2024-01-15 12:00:00'                    | 7962214294659802846  |
        | timestamp from string      | UTC                 | CAST('2024-01-15 12:00:00' AS TIMESTAMP)           | 7962214294659802846  |
        | timestamp_ntz              | UTC                 | TIMESTAMP_NTZ '2024-01-15 12:00:00'                | 7962214294659802846  |
        | date                       | UTC                 | DATE '2024-01-15'                                  | 2166432641145730595  |
        | timestamp in array         | UTC                 | array(TIMESTAMP '2024-01-15 12:00:00')             | 7962214294659802846  |
        | timestamp in struct        | UTC                 | named_struct('t', TIMESTAMP '2024-01-15 12:00:00') | 7962214294659802846  |
        | timestamp                  | America/Los_Angeles | TIMESTAMP '2024-01-15 12:00:00'                    | -2005686892988844055 |
        | timestamp from string      | America/Los_Angeles | CAST('2024-01-15 12:00:00' AS TIMESTAMP)           | -2005686892988844055 |
        | timestamp_ntz              | America/Los_Angeles | TIMESTAMP_NTZ '2024-01-15 12:00:00'                | 7962214294659802846  |
        | date                       | America/Los_Angeles | DATE '2024-01-15'                                  | 2166432641145730595  |
        | timestamp in array         | America/Los_Angeles | array(TIMESTAMP '2024-01-15 12:00:00')             | -2005686892988844055 |
        | timestamp in struct        | America/Los_Angeles | named_struct('t', TIMESTAMP '2024-01-15 12:00:00') | -2005686892988844055 |
        | timestamp                  | Asia/Kolkata        | TIMESTAMP '2024-01-15 12:00:00'                    | -3553635892426643839 |
        | timestamp from string      | Asia/Kolkata        | CAST('2024-01-15 12:00:00' AS TIMESTAMP)           | -3553635892426643839 |
        | timestamp_ntz              | Asia/Kolkata        | TIMESTAMP_NTZ '2024-01-15 12:00:00'                | 7962214294659802846  |
        | date                       | Asia/Kolkata        | DATE '2024-01-15'                                  | 2166432641145730595  |
        | timestamp in array         | Asia/Kolkata        | array(TIMESTAMP '2024-01-15 12:00:00')             | -3553635892426643839 |
        | timestamp in struct        | Asia/Kolkata        | named_struct('t', TIMESTAMP '2024-01-15 12:00:00') | -3553635892426643839 |
        | timestamp                  | Europe/Madrid       | TIMESTAMP '2024-01-15 12:00:00'                    | -236999359730609280  |
        | timestamp from string      | Europe/Madrid       | CAST('2024-01-15 12:00:00' AS TIMESTAMP)           | -236999359730609280  |
        | timestamp_ntz              | Europe/Madrid       | TIMESTAMP_NTZ '2024-01-15 12:00:00'                | 7962214294659802846  |
        | date                       | Europe/Madrid       | DATE '2024-01-15'                                  | 2166432641145730595  |
        | timestamp in array         | Europe/Madrid       | array(TIMESTAMP '2024-01-15 12:00:00')             | -236999359730609280  |
        | timestamp in struct        | Europe/Madrid       | named_struct('t', TIMESTAMP '2024-01-15 12:00:00') | -236999359730609280  |
        | dst gap in Los Angeles     | America/Los_Angeles | TIMESTAMP '2024-03-10 02:30:00'                    | 2430670695775041828  |
        | dst overlap in Los Angeles | America/Los_Angeles | TIMESTAMP '2024-11-03 01:30:00'                    | -3179529028413526341 |

    Scenario: Time zone: a timestamp column in UTC
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (TIMESTAMP '2024-01-15 12:00:00'), (TIMESTAMP '2024-07-15 12:00:00'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query result ordered
        | result               |
        | 7962214294659802846  |
        | -5340696876632206268 |
        | 42                   |

    Scenario: Time zone: a timestamp column in America/Los_Angeles
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (TIMESTAMP '2024-01-15 12:00:00'), (TIMESTAMP '2024-07-15 12:00:00'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -2005686892988844055 |
        | -2297178526965451109 |
        | 42                   |

    Scenario: Time zone: a timestamp column in Asia/Kolkata
      Given config spark.sql.session.timeZone = Asia/Kolkata
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (TIMESTAMP '2024-01-15 12:00:00'), (TIMESTAMP '2024-07-15 12:00:00'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -3553635892426643839 |
        | 4233783053213945953  |
        | 42                   |

    Scenario: Time zone: a timestamp column in Europe/Madrid
      Given config spark.sql.session.timeZone = Europe/Madrid
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES (TIMESTAMP '2024-01-15 12:00:00'), (TIMESTAMP '2024-07-15 12:00:00'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query result ordered
        | result               |
        | -236999359730609280  |
        | -6701245572596551638 |
        | 42                   |

  Rule: Default timestamp type

    # Default: spark.sql.timestampType = TIMESTAMP_LTZ, so the TIMESTAMP keyword is a local-time instant.
    # spark.sql.timestampType decides whether the TIMESTAMP keyword is LTZ or NTZ, and so whether the zone
    # reaches the hash.
    # Only the zone is set: the TIMESTAMP keyword is LTZ by default, so the zone reaches the hash
    # (an NTZ timestamp would hash 7962214294659802846 in every zone).
    Scenario: Timestamp type: the default is TIMESTAMP_LTZ
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT xxhash64(TIMESTAMP '2024-01-15 12:00:00') AS result
        """
      Then query result
        | result               |
        | -2005686892988844055 |

    Scenario Outline: Timestamp type: <case> in <zone>
      Given config spark.sql.timestampType = <type>
      And config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case               | type          | zone                | arg                                      | result               |
        | TIMESTAMP LTZ      | TIMESTAMP_LTZ | UTC                 | TIMESTAMP '2024-01-15 12:00:00'          | 7962214294659802846  |
        | TIMESTAMP LTZ cast | TIMESTAMP_LTZ | UTC                 | CAST('2024-01-15 12:00:00' AS TIMESTAMP) | 7962214294659802846  |
        | TIMESTAMP NTZ      | TIMESTAMP_NTZ | UTC                 | TIMESTAMP '2024-01-15 12:00:00'          | 7962214294659802846  |
        | TIMESTAMP NTZ cast | TIMESTAMP_NTZ | UTC                 | CAST('2024-01-15 12:00:00' AS TIMESTAMP) | 7962214294659802846  |
        | TIMESTAMP LTZ      | TIMESTAMP_LTZ | America/Los_Angeles | TIMESTAMP '2024-01-15 12:00:00'          | -2005686892988844055 |
        | TIMESTAMP LTZ cast | TIMESTAMP_LTZ | America/Los_Angeles | CAST('2024-01-15 12:00:00' AS TIMESTAMP) | -2005686892988844055 |
        | TIMESTAMP NTZ      | TIMESTAMP_NTZ | America/Los_Angeles | TIMESTAMP '2024-01-15 12:00:00'          | 7962214294659802846  |
        | TIMESTAMP NTZ cast | TIMESTAMP_NTZ | America/Los_Angeles | CAST('2024-01-15 12:00:00' AS TIMESTAMP) | 7962214294659802846  |

  Rule: Legacy interval flag

    # Default: spark.sql.legacy.interval.enabled = false, so subtracting timestamps yields INTERVAL DAY TO SECOND.
    # With spark.sql.legacy.interval.enabled the subtraction of timestamps or dates yields a calendar
    # interval instead of INTERVAL DAY TO SECOND, so the hash of the difference changes. The flag is read
    # by the subtraction, not by xxhash64: make_interval is not affected.
    Scenario: Interval flag: the default is false
      When query
        """
        SELECT xxhash64(TIMESTAMP '2024-01-02 03:04:05' - TIMESTAMP '2024-01-01 00:00:00') AS result
        """
      Then query result
        | result               |
        | -6439795666018909673 |

    Scenario Outline: Interval flag <flag>: <case>
      Given config spark.sql.legacy.interval.enabled = <flag>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | flag  | arg                                                               | result               |
        | timestamp subtraction | false | TIMESTAMP '2024-01-02 03:04:05' - TIMESTAMP '2024-01-01 00:00:00' | -6439795666018909673 |
        | make_interval         | false | make_interval(1, 2, 3, 4)                                         | -2276548691635096493 |
        | make_interval         | true  | make_interval(1, 2, 3, 4)                                         | -2276548691635096493 |

    # TODO(spark.sql.legacy.interval.enabled): Sail does not read the flag, and DATE - DATE does not
    # yield Spark's interval type with either value, so these hashes differ. Fix the subtraction first.
    @sail-bug
    Scenario Outline: Interval flag <flag>: <case>
      Given config spark.sql.legacy.interval.enabled = <flag>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | flag  | arg                                                               | result              |
        | date subtraction      | false | DATE '2024-01-10' - DATE '2024-01-01'                             | 349260374026122526  |
        | timestamp subtraction | true  | TIMESTAMP '2024-01-02 03:04:05' - TIMESTAMP '2024-01-01 00:00:00' | 4703337699461816782 |
        | date subtraction      | true  | DATE '2024-01-10' - DATE '2024-01-01'                             | -358250312222152650 |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal yields a non-nullable bigint
      When query
        """
        SELECT xxhash64('a') AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a non-null column yields a non-nullable bigint
      When query
        """
        SELECT xxhash64(id) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable column: xxhash64 is still non-nullable
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES ('a'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """
