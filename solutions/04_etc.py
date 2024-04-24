import time

from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = (
    SparkSession.builder.master("local[5]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Exercise1")
    .getOrCreate()
)

products_table = spark.read.parquet("exercises/data/products_parquet/")
sales_table = spark.read.parquet("exercises/data/sales_parquet/")

joined_table = sales_table.join(products_table, sales_table.product_id == products_table.product_id)

result = joined_table.groupBy("date").agg(
    avg(joined_table["price"] * joined_table["num_pieces_sold"]),
)

result.explain(mode='formatted')

result.collect()

time.sleep(6000)

# Create spark session with local[5]
rdd = spark.sparkContext.parallelize(range(0,20))
print("From local[5] : "+str(rdd.getNumPartitions()))

# Use parallelize with 6 partitions
rdd1 = spark.sparkContext.parallelize(range(0,25), 6)
print("parallelize : "+str(rdd1.getNumPartitions()))

rddFromFile = spark.sparkContext.textFile("exercises/data/words.txt", 10)
print("TextFile : "+str(rddFromFile.getNumPartitions()))

rddFromFile.saveAsTextFile("tmp/partition")

# Using repartition
rdd2 = rdd1.repartition(4)
print("Repartition size : "+str(rdd2.getNumPartitions()))
rdd2.saveAsTextFile("tmp/re-partition")

# Using coalesce()
rdd3 = rdd1.coalesce(4)
print("Repartition size : "+str(rdd3.getNumPartitions()))
rdd3.saveAsTextFile("tmp/coalesce")