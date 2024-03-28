from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *

spark = (
    SparkSession.builder.master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "3gb")
    .appName("Exercise1")
    .getOrCreate()
)

# sales_table = spark.read.parquet("exercises/data/sales_parquet/")

# tmp = sales_table.groupBy(col("product_id"), col("seller_id")).agg(
#     sum("num_pieces_sold").alias("num_pieces_sold"),
# )

# window_desc = Window.partitionBy(col("product_id")).orderBy(
#     col("num_pieces_sold").desc(),
# )
# window_asc = Window.partitionBy(col("product_id")).orderBy(
#     col("num_pieces_sold").asc(),
# )

# # dense rank does not create holes because of ties
# tmp = tmp.withColumn("rank_asc", dense_rank().over(window_asc))
# tmp = tmp.withColumn("rank_desc", dense_rank().over(window_desc))

# # There is a bug in the pdf here!
# single_seller = tmp.where(
#     (col("rank_asc") == col("rank_desc")) & (col("rank_asc") == 1),
# ).select(
#     col("product_id").alias("single_seller_product_id"),
#     col("seller_id").alias("single_seller_seller_id"),
#     col("rank_asc"),
#     lit("Only seller or all sellers with the same result").alias("type"),
# )

# single_seller.show()

# second_seller = tmp.where(col("rank_desc") == 2).select(
#     col("product_id").alias("second_seller_product_id"),
#     col("seller_id").alias("second_seller_seller_id"),
#     lit("Second top seller").alias("type"),
# )

# least_seller = (
#     tmp.where(col("rank_asc") == 1)
#     .select(
#         col("product_id"),
#         col("seller_id"),
#         lit("Least Seller").alias("type"),
#     )
#     .join(
#         single_seller,
#         (tmp["seller_id"] == single_seller["single_seller_seller_id"])
#         & (tmp["product_id"] == single_seller["single_seller_product_id"]),
#         "left_anti",
#     )
#     .join(
#         second_seller,
#         (tmp["seller_id"] == second_seller["second_seller_seller_id"])
#         & (tmp["product_id"] == second_seller["second_seller_product_id"]),
#         "left_anti",
#     )
# )

# union_table = (
#     least_seller.select(
#         col("product_id"),
#         col("seller_id"),
#         col("type"),
#     )
#     .union(
#         second_seller.select(
#             col("second_seller_product_id").alias("product_id"),
#             col("second_seller_seller_id").alias("seller_id"),
#             col("type"),
#         ),
#     )
#     .union(
#         single_seller.select(
#             col("single_seller_product_id").alias("product_id"),
#             col("single_seller_seller_id").alias("seller_id"),
#             col("type"),
#         ),
#     )
# )

# union_table.show()

# union_table.where(col("product_id") == 0).show()

# Extra

# data = [('James', 34, 55000), ('Michael', 30, 70000), ('Robert', 37, 60000), ('Maria', 29, 80000), ('Jen', 32, 65000)]
# df = spark.createDataFrame(data, ["name", "age" , "salary"])

# df = df.withColumn("id", monotonically_increasing_id())

# window = Window.orderBy("id")
# df = df.withColumn("prev_value", lag(df.salary).over(window))
# df = df.withColumn("diff", df.salary - df.prev_value)

# df.show()

data = [
    Row(id=1, column1=5),
    Row(id=2, column1=8),
    Row(id=3, column1=12),
    Row(id=4, column1=1),
    Row(id=5, column1=15),
    Row(id=6, column1=7),
]
df = spark.createDataFrame(data)
df.show()

window = Window.orderBy(desc("column1"))
df = df.withColumn("row_number", row_number().over(window))

n = 3
row = df.filter(df.row_number == n).first()

print(row)
