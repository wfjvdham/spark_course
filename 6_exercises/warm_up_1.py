from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()     

products_table = spark.read.parquet("6_exercises/data/products_parquet/")
products_table.createOrReplaceTempView("products")

sellers_table = spark.read.parquet("6_exercises/data/sellers_parquet/")
sellers_table.createOrReplaceTempView("sellers")

sales_table = spark.read.parquet("6_exercises/data/sales_parquet/")
sales_table.createOrReplaceTempView("sales")

n_products = spark.sql("SELECT COUNT(*) FROM products")
print('n_products')
n_products.show()
print(products_table.count())

n_sellers = spark.sql("SELECT COUNT(*) FROM sellers")
print('n_sellers')
n_sellers.show()
print(sellers_table.count())

answer = spark.sql("SELECT COUNT(DISTINCT(product_id)) FROM sales")
print("products sold at least once")
answer.show()
sales_table.agg(countDistinct(col("product_id"))).show()

answer = spark.sql("""
    SELECT product_id, COUNT(*) AS n_products 
    FROM sales
    GROUP BY product_id
    ORDER BY n_products DESC
    LIMIT 1;
""")
print("product in most orders")
answer.show()
sales_table.groupBy(col("product_id")).agg(
    count("*").alias("n_products")
).orderBy(col("n_products").desc()).limit(1).show()