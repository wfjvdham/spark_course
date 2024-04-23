# Explain

[link](https://medium.com/@shuklaprashant9264/pyspark-explain-physical-logical-plan-bf8e25d80fcf)

- **Physical Plan** Detailed order of execution of different steps
- **Logical Plan** More high level logical steps to take
- **Cost Estimates** cost of a query based on data size and complexity
- **Optimizations** Optimizations that spark has already applied

`mode="formatted"` gives a much cleaner plan

# Shuffling

When spark needs to shuffle, data is reorganized across partitions.

Typically triggered by `groupByKey`,  `groupBy`, `join`, `repartition`

Try to reduce the number of shuffles you need, it is costly and prevents continuing with the parallel proces

Less machines with larges resources feel less pain of a shuffle

filter before shuffles to reduce the impact of the shuffle

# Caching

Memory is defided into storage memory and execution memory

Store a df into storage memory only when you have enough memory for storage and execution, otherwise spark will start writing to disk wich makes it slow

Efective if a df is used repeadedly in different steps

Do not cache directly after reading parquet file, spark only reads the columns it needs

