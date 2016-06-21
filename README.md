# fortigate-log-parser-in-spark
FortiGate syslog parser in pyspark

## Sample of Fortigate Log
```
May 22 00:00:01 192.168.1.1 date=2016-05-22 time=00:00:01 devname=aaa-qooo ...
```

## How to use
SPARK_HOME/bin/spark-submit --driver-memory 1g --executor-memory 1g --master yarn logparser_spark.py PATH_TO_LOG_FILE PATH_TO_OUTPUT_FILE

* PATH_TO_LOG_FILE and PATH_TO_OUTPUT_FILE are both in HDFS
* OUTPUT_FILE is in *PARQUET* format