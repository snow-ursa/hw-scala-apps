## HW2: Scala + Hadoop FileSystem API

### Task
https://github.com/Gorini4/hadoop_course_homework/tree/master/hw1

### HDFS (before changes)
```bash
root@datanode:/# hdfs dfs -ls -R /
drwxrwxrwx   - root      supergroup          0 2023-04-11 08:12 /stage
drwxrwxrwx   - root      supergroup          0 2023-04-11 08:12 /stage/date=2020-12-01
-rwxrwxrwx   3 root      supergroup       6148 2023-04-11 08:12 /stage/date=2020-12-01/.DS_Store
-rwxrwxrwx   3 root      supergroup          0 2023-04-11 08:12 /stage/date=2020-12-01/part-0000.csv
-rwxrwxrwx   3 root      supergroup        596 2023-04-11 08:12 /stage/date=2020-12-01/part-0001.csv.inprogress
drwxrwxrwx   - root      supergroup          0 2023-04-11 08:12 /stage/date=2020-12-02
-rwxrwxrwx   3 root      supergroup       1740 2023-04-11 08:12 /stage/date=2020-12-02/part-0000.csv.inprogress
drwxrwxrwx   - root      supergroup          0 2023-04-11 08:12 /stage/date=2020-12-03
-rwxrwxrwx   3 root      supergroup       2595 2023-04-11 08:12 /stage/date=2020-12-03/part-0000.csv
-rwxrwxrwx   3 root      supergroup        634 2023-04-11 08:12 /stage/date=2020-12-03/part-0001.csv
-rwxrwxrwx   3 root      supergroup       1332 2023-04-11 08:12 /stage/date=2020-12-03/part-0002.csv
drwxrwxrwx   - root      supergroup          0 2023-04-11 08:12 /stage/dd
-rwxrwxrwx   3 root      supergroup       2595 2023-04-11 08:12 /stage/dd/part-0000.csv
-rwxrwxrwx   3 root      supergroup       1332 2023-04-11 08:12 /stage/dd/part-0001.csv

```

### HDFS (after changes)
```bash
drwxr-xr-x   - n_kurkina supergroup          0 2023-04-11 08:25 /ods
drwxr-xr-x   - n_kurkina supergroup          0 2023-04-11 08:25 /ods/date=2020-12-01
-rw-r--r--   3 n_kurkina supergroup        596 2023-04-11 08:25 /ods/date=2020-12-01/part.csv
drwxr-xr-x   - n_kurkina supergroup          0 2023-04-11 08:25 /ods/date=2020-12-02
-rw-r--r--   3 n_kurkina supergroup       1740 2023-04-11 08:25 /ods/date=2020-12-02/part.csv
drwxr-xr-x   - n_kurkina supergroup          0 2023-04-11 08:25 /ods/date=2020-12-03
-rw-r--r--   3 n_kurkina supergroup       4561 2023-04-11 08:25 /ods/date=2020-12-03/part.csv
drwxrwxrwx   - root      supergroup          0 2023-04-11 08:25 /stage

```
