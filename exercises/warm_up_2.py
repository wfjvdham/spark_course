from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder.master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Exercise1")
    .getOrCreate()
)

products_table = spark.read.parquet("6_exercises/data/products_parquet/")
sellers_table = spark.read.parquet("6_exercises/data/sellers_parquet/")
sales_table = spark.read.parquet("6_exercises/data/sales_parquet/")

sales_table.groupBy(col("date")).agg(
    countDistinct(col("product_id")).alias("cnt"),
).orderBy(col("cnt").desc()).show()
