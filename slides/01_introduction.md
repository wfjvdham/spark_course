# About Myself

Wim van der Ham

mono@fastmail.nl

Data Engineer Alliander

# Experience with PySpark:

- Quby
- LeasePlan

# About You?

- What is your level of experience with pySpark?
- What would you like to learn in this course?

# About This Course

- Flexible
    - breaks
    - coffee snacks
    - use cases
- Practical
- [GitHub](https://github.com/wfjvdham/spark_course)

# About Spark

[Spark](https://spark.apache.org/)

Distributed computing Framework

- **Scalability** Spark scales horizontally, allowing users to easily scale their applications from a single machine to thousands of nodes. It achieves scalability by distributing data across the cluster and parallelizing computation.
- **Lazy Evaluation** Spark adopts a lazy evaluation strategy, meaning it delays executing transformations until an action is triggered. This optimization technique improves performance by allowing Spark to optimize the execution.
- **In-Memory Computing** Spark's in-memory computing capabilities enable it to cache frequently accessed data in memory, which significantly accelerates iterative algorithms and interactive data analysis.
- **Ease of Use** Spark provides high-level APIs in languages like Scala, Java, Python, and R, making it accessible to a wide range of developers with different skill sets. It also offers a rich set of built-in libraries for various tasks like SQL queries (Spark SQL), machine learning (MLlib), graph processing (GraphX), and stream processing (Spark Streaming).

# About pySpark

[pySpark](https://spark.apache.org/docs/latest/api/python/index.html)

python package that allows you to interact with Spark

# Installation

1. Install wsl if on windows:

`wsl --install`

2. install java if it is not installed already:

```
java -version
sudo apt update
sudo apt install default-jre
java -version
```

3. Install `pipx` required for installing `poetry`

```
sudo apt install pipx
pipx ensurepath
```

4. Install `poetry`

```
pipx install poetry
```

5. Install python dependencies and `pyspark`:

```
poetry install
```
