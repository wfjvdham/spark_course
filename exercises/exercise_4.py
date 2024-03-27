import hashlib

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder.master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "3gb")
    .appName("Exercise1")
    .getOrCreate()
)

sales_table = spark.read.parquet("6_exercises/data/sales_parquet/")


def algo(order_id, bill_text):

    ret = bill_text.encode("utf-8")
    if int(order_id) % 2 == 0:
        cnt_A = bill_text.count("A")
        for _c in range(0, cnt_A):
            ret = hashlib.md5(ret).hexdigest().encode("utf-8")
        ret = ret.decode("utf-8")
    else:
        ret = hashlib.sha256(ret).hexdigest()
    return ret


algo_udf = spark.udf.register("algo", algo)

sales_table.withColumn(
    "hashed_bill",
    algo_udf(col("order_id"), col("bill_raw_text")),
).groupBy(col("hashed_bill")).agg(count("*").alias("cnt")).where(col("cnt") > 1).show()
