# Hadoop

Hadoop is a distributed file storage system with a data processing engine MapReduce.

MapReduce is not as fast as Spark and can only process batch data.

So if you use hadoop you can better combine it with Spark

# Do you need Hadoop if you use Spark

Spark does not have a distributed data storage. If you need that you can use hadoop.

# locally run with multiple node

```master("local[3]")```

# Spark Web UI

DAG
Event Timeline
SQL / DataFrame

Shuffle Read Total bytes read during shuffle
Shuffle Write Bytes writen to disk in order to be read by a future shuffle

# Etc

https://sparkbyexamples.com/spark/spark-web-ui-understanding/

https://sparkbyexamples.com/pyspark/pyspark-repartition-vs-coalesce/

https://github.com/spark-examples/pyspark-examples/blob/master/pyspark-types.py

https://sparkbyexamples.com/pyspark/pyspark-broadcast-variables/

https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/

https://sparkbyexamples.com/pyspark/pyspark-window-functions/