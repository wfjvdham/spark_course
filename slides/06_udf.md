# User Defined Functions

A function to calculate something

WARNING: in general it is not recommended to use them because Spark cannot optimize them. Always first try to express it in SQL

But if you cannot do it you have to use UDF

```
def do_something(var_id):

    ret = "6"
    if int(var_id) % 2 == 0:
        ret = "5"

    return ret


udf = spark.udf.register("do_something", do_something)

table.withColumn("new_var_id", udf(col("var_id")))
```

# Exercise 4

- hashlib.md5(ret).hexdigest()
- hashlib.sha256(ret).hexdigest()
