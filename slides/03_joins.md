# Different types of Joins

[article](https://towardsdatascience.com/the-art-of-joining-in-spark-dcbd33d693c)

- Sort Merge Joins
- Broadcast Joins
- Salted Key Joins (explained later)

# Sort Merge Joins

- Sorting
- Merging
- All-to-all communication
- Minimize data movement
- But can be slow when the join key is skewed

# Broadcast Joins

- Copies the smallest table to the memory of all nodes
- Can be fast but make sure the *small* table stays *small*
- Spark does this automatically expect when you set the threshold

```
.config("spark.sql.autoBroadcastJoinThreshold", -1)
```

- Can be forced by adding `broadcast()`

```
big_table.join(broadcast(small_table), ...)
```

# withColumn

- function used to add a new column to a DataFrame

```
df = df.withColumn("is_adult", when(df["age"] >= 18, "Yes").otherwise("No"))
```

# Exercise 2 Extra

- In the sales table keep the 2 most occurring dates, replace the other dates with `Other`

You can use `flatMap()` [Explained](https://stackoverflow.com/a/22510434)

- Try the
