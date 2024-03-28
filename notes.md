- explain

use the explain() function after a query

- repartition
- where == filter
- replace aggregate and join by window

# first approach
df_agg = df.groupBy('city','team').agg(F.mean('job').alias('job_mean'))
df = df.join(df_agg, on=['city', 'team'], how='inner')
# second approach
from pyspark.sql.window import Window
window_spec = Window.partitionBy(df['city'], df['team'])
df = df.withColumn('job_mean', F.mean(col('job')).over(window_spec))

- min shuffles

101:
after 40

MAYBE:
- coalesce
- mapFlatten
- reduceByKey
- aggregate
- RDD to DF, schema?
- to pandas?
- arrow?
- bucketing

NOT:
- streaming (kafka)
- GraphX
-
