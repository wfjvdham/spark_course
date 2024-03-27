# Salted Key Joins

- add some random data to the most occurring keys
- This will improve the balancing across nodes

# RRD

Resilient Distributed Dataset, are abstractions that are more low level then DataFrames

- **Resilient** RDDs are resilient because they can automatically recover from failures. Spark tracks the lineage of transformations applied to RDDs, allowing it to reconstruct lost data partitions in case of node failures.
- **Distributed** RDDs are distributed across multiple nodes in a cluster, enabling parallel processing of data. Spark transparently handles data partitioning and distribution, allowing operations on RDDs to be executed in parallel across the cluster.

Can be created in two ways:

1. Reading the data
1. Using `parallelize()` or other transformations

```
rdd1 = sc.parallelize([1, 2, 3, 4, 5])
rdd2 = rdd1.map(lambda x: x * 2)
```

# Spark WebUI

[WebUI](https://spark.apache.org/docs/latest/web-ui.html)

Apache Spark provides a suite of web user interfaces (UIs) that you can use to monitor the status and resource consumption of your Spark cluster.

- **Jobs** are run every time after a `show()`, `count()` or `collect()` is called
- **Stages** a set of tasks that can be executed in parallel. Divided by when data needs to be exchanged.


# Exercise 1

- Create a table with the 100 most occurring products
- Create a list with those ids combined with a number between 0 and 100
- Turn this list into an rdd
- Turn this rdd into a DataFrame using: `map(lambda x: Row(...))`
- Update the product_ids in the product and sales tables
- Do the join using the new keys
- Do the required computation
- Check in the SparkUI if the results look better
