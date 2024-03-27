# SparkSession

- Entry point to the Spark functionality
- Configuration
- DataFrame API
- Spark SQL

```
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()
```

# DataFrame API

```
from pyspark.sql.functions import *

users_table.groupBy(col("user_id")).agg(
    count("*").alias("n_users"),
    avg("clicks").alias("avg_clicks")
).orderBy(col("avg_clicks").desc()).limit(1).show()
```

# Spark SQL

```
spark.sql("SELECT COUNT(*) FROM users")
```

# parquet files

- Columnar Storage, mostly faster
- Compressed
- Schema

```
spark.read.parquet("<location of file or folder>")
```

# Warmup 1 & 2
