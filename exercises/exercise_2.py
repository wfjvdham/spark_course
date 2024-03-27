from __future__ import annotations

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder.master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Exercise1")
    .getOrCreate()
)

sellers_table = spark.read.parquet("6_exercises/data/sellers_parquet/")
sales_table = spark.read.parquet("6_exercises/data/sales_parquet/")

joined_table = sales_table.join(
    broadcast(sellers_table),
    sales_table.seller_id == sellers_table.seller_id,
)

joined_table.withColumn(
    "contribution",
    col("num_pieces_sold") / col("daily_target"),
).groupBy(sellers_table["seller_id"]).agg({"contribution": "avg"}).show()

time.sleep(6000)
