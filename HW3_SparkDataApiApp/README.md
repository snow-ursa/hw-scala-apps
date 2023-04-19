## HW3: DataFrame + RDD + Dataset

### Task
https://github.com/vzaigrin/otus/tree/main/SparkHomework/HW3

### Task 1: DataFrame
```bash
+-------------+----------+
|      Borough|TripsCount|
+-------------+----------+
|    Manhattan|    296527|
|       Queens|     13819|
|     Brooklyn|     12672|
|      Unknown|      6714|
|        Bronx|      1589|
|          EWR|       508|
|Staten Island|        64|
+-------------+----------+
```

### Task 2: RDD
```bash
09:51:56 18
19:48:51 17
19:54:47 17
14:32:01 17
22:38:40 17
18:24:54 16
19:11:41 16
09:25:26 16
20:47:38 15
15:39:38 15
21:46:07 15
...
```

### Task 3: Dataset
```bash
pg_test_db=# select * from distance_distribution ;
    borough    | trips_count |   mean_distance    |    std_distance    | min_distance | max_distance 
---------------+-------------+--------------------+--------------------+--------------+--------------
 Manhattan     |      296527 | 2.1818937229998903 | 2.6307171623374805 |            0 |        37.92
 Queens        |       13819 |  8.712971995079211 |  5.557884808639987 |            0 |         51.6
 Brooklyn      |       12672 |   6.88587436868688 | 4.7724725598440365 |            0 |         44.8
 Unknown       |        6714 |  3.451726243669959 |  5.603374652105368 |            0 |           66
 Bronx         |        1589 |  9.052913782253006 |   5.41745600838284 |            0 |        31.18
 EWR           |         508 |  16.97218503937008 |  4.864870222043952 |            0 |        45.98
 Staten Island |          64 | 19.485468749999995 |   7.64428429066242 |            0 |        33.78
(7 rows)
```
