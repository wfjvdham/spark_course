from pyspark.ml.feature import Imputer
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder.master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "3gb")
    .appName("Exercise1")
    .getOrCreate()
)

df = spark.createDataFrame(
    [
        ("A", 1, None),
        ("B", None, 123),
        ("B", 3, 456),
        ("D", 6, None),
    ],
    ["Name", "var1", "var2"],
)

df.show()

column_names = ["var1", "var2"]

mean_imputer = Imputer(inputCols=column_names, outputCols=column_names)

model = mean_imputer.fit(df)

imputed_df = model.transform(df)
imputed_df.show()
