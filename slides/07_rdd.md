# When to use a RDD

- When data is unstructured text
- Low level transformations
- Schema is not important

# When to use DataFrames

- When the data has a structure
- High level processing

# RDD Functions

- **map()** applies a function to every row in a rdd and returns the same number of rows
- **flatMap()** applies a function to every row in a rdd and flattens the result is 1 column so can return more then the number of rows
- **reduceByKey()** reduces all values for a specific key to 1 agregated value per key
- **sortByKey()** sorts the results by key
- **filter()** filter the results using a condition

# Use Case: Wordcount

- Make a text file with some words in it
- Read the text file using `spark.sparkContext.textFile()`
- Loop over the result to see what is in it
- Split every line in words using a lambda function and `flatMap()`
- Combine this result and add it to a tuple together with number 1 using `map()`
- Use `reduceByKey()` to count for every word
- Flip key and value
- Sort results by number of occurences, use `sortByKey()`
- Filter for words with an `a`

# Use Case: Convertion to DF

- Turn this data into an RDD:

```
dept = [("Finance",10), 
        ("Marketing",20), 
        ("Sales",30), 
        ("IT",40) 
      ]
```

- Turn the rdd into a df and print the schema
- Turn the rdd into a df, specify the column names and print the schema
- Use `createDataFrame` with a schema argument