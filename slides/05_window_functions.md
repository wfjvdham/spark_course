# Window Functions

Functions that perform calculation and optionally an ordering within a specific partition

```
SELECT
    department_id,
    employee_id,
    salary,
    AVG(salary) OVER (PARTITION BY department_id ORDER BY salary DESC) AS avg_salary_per_department
FROM
    employees;
```

```
window_avg_salary_per_department = Window.partitionBy(col("department_id")).orderBy(col("salary").desc())

employees_table.select(
    department_id,
    employee_id,
    salary,
    avg(salary).over(window_avg_salary_per_department).alias("avg_salary_per_department")
)
```

other examples of build in functions: [link](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#aggregate-functions)


# Anti Join

A way to filter rows from a table

![Anti Join](left-anti-join-operation.png)

# union

A function that combines the rows of two DataFrames

```
df1 = spark.createDataFrame([(1, 'A'), (2, 'B')], ['id', 'value'])
df2 = spark.createDataFrame([(3, 'C'), (4, 'D')], ['id', 'value'])
df3 = df1.union(df2)
df3.show()
```

# Exercise 3

- Create a table that groups by product and seller
- Define window functions for the ordering
- Use `dense_rank()`
- Create a table for the exceptions
- Create two tables for the objectives
- Combine all those tables to get the answer
- Show the answer for product 0
