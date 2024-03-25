- what is spark

Apache most popular
distributed, parallel
Big Data

- different API's

python <- easier if you already know python
scala <- performs best
java

- How spark works



- sparkSession
- parquet files
- table
    - SQL
    - Spark functions
- stages
- warmup 1 & 2

- joins

https://towardsdatascience.com/the-art-of-joining-in-spark-dcbd33d693c

data skewness joins means that most of the work is done on executer

- Sort Merge Joins

all-to-all communication
sorts the data (Heavy)
minimize data movement

- Broadcast Joins

Send a copy of the table to all nodes
no more communication required but data dupliction

- withColumn
- agg
- sparkUI

- RDD

lazily evaluated
statically typed
distributed collections

- Spark Job

Lazy means only does something when the driver asks for it.
Job -> stages -> tasks
1 task per partition per rdd
tasks do the same code on different pieces of data

after communication a new stage is required
after shuffeling (sort or groubBy)

- history 

https://sparkbyexamples.com/spark/spark-setup-on-hadoop-yarn/#spark-history-server

