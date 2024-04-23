from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import timeit

# Create a Spark session
spark = SparkSession.builder.appName("ExplainExample").getOrCreate()

# products_table = spark.read.parquet("exercises/data/products_parquet/")
# sales_table = spark.read.parquet("exercises/data/sales_parquet/")
# sellers_table = spark.read.parquet("exercises/data/sellers_parquet/")

# joined_table = sales_table.join(products_table, sales_table.product_id == products_table.product_id)

# joined_table.explain(mode="formatted")

# joined_table = sales_table.join(
#     broadcast(sellers_table),
#     sales_table.seller_id == sellers_table.seller_id,
# )

# joined_table.explain(mode="formatted")

# product_sold = sales_table.groupBy("product_id").agg(sum("num_pieces_sold").alias("total_sold"))
# product_sold.explain(mode="formatted")

# Sample data
data = [("Alice", 1), ("Bob", 2), ("Alice", 3), ("Bob", 4)]
columns = ["Name", "Value"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Perform an operation that requires a shuffle
result = df.groupBy("Name").sum("Value")

# Show the execution plan
result.explain(mode="formatted")

# Method 1: Without caching
def without_caching():
    result = df.filter(col("Value") > 1).groupBy("Name").count()
    result.explain()
    result.count()

# Method 2: With caching
def with_caching():
    df.persist() # Cache the DataFrame
    result = df.filter(col("Value") > 1).groupBy("Name").count()
    result.explain()
    result.count()
    df.unpersist() # Unpersist the cached DataFrame

# Measure time without caching
print("Performance without caching:")
print(timeit.timeit(stmt='without_caching()', globals=globals(), number=1))

# Measure time with caching
print("\nPerformance with caching:")
print(timeit.timeit(stmt='with_caching()', globals=globals(), number=1))