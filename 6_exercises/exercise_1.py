import time

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()  

products_table = spark.read.parquet("6_exercises/data/products_parquet/")
sellers_table = spark.read.parquet("6_exercises/data/sellers_parquet/")
sales_table = spark.read.parquet("6_exercises/data/sales_parquet/")

start_time = time.monotonic()

# works but slow

# joined_table = sales_table.join(products_table, sales_table.product_id == products_table.product_id)

# table is too big

# small_sales_table = sales_table.orderBy(rand()).limit(1000)

# joined_table = products_table.join(broadcast(small_sales_table), sales_table.product_id == products_table.product_id)

# key salting

results = sales_table.groupBy(sales_table["product_id"]).count().sort(col("count").desc()).limit(100).collect()

# print(results)

REPLICATION_FACTOR = 101
l = []
replicated_products = []

for _r in results:
    replicated_products.append(_r["product_id"])
    for _rep in range(0, REPLICATION_FACTOR):
        l.append((_r["product_id"], _rep))
rdd = spark.sparkContext.parallelize(l)
replicated_df = rdd.map(
    lambda x: Row(product_id=x[0], replication = int(x[1]))
)
replicated_df = spark.createDataFrame(replicated_df)

products_table = products_table.join(
    broadcast(replicated_df), products_table["product_id"] == replicated_df["product_id"], "left"
).withColumn("salted_join_key", when(
    replicated_df["replication"].isNull(),
    products_table["product_id"] 
).otherwise(concat(replicated_df["product_id"], lit("-"), replicated_df["replication"]))
)

# products_table.show()

sales_table = sales_table.withColumn(
    "salted_join_key", 
    when(
        sales_table["product_id"].isin(replicated_products), 
        concat(
            sales_table["product_id"], lit("-"), round(
                rand() * (REPLICATION_FACTOR - 1), 0
            ).cast(IntegerType())
        )
    ).otherwise(sales_table["product_id"])
)

# sales_table.show()

joined_table = sales_table.join(
    products_table, 
    sales_table["salted_join_key"] == products_table["salted_join_key"]
)

# joined_table.show()

joined_table.agg(avg(joined_table["price"] * joined_table["num_pieces_sold"])).show()

# joined_table.withColumn("revenue", col("num_pieces_sold") * col("price")).agg({"revenue": "avg"}).show()

print(time.monotonic() - start_time)

time.sleep(6000)