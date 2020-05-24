from pyspark.sql.functions import coalesce, col, when, lag, sum
from pyspark.sql.window import Window

window_1 = Window.orderBy("Start").partitionBy("User_ID")
window_2 = Window.orderBy("End").partitionBy("User_ID")

df_1 = spark.createDataFrame([(101, "Big Boss", 1, 10), (101, "Big Boss", 5, 15), (101, "Big Boss", 50, 60), (102, "Big Boss", 1, 11), (102, "Big Boss", 30, 40)], ("User_ID", "Episode_Name", "Start", "End"))
df_1.show()
+-------+------------+-----+---+
|User_ID|Episode_Name|Start|End|
+-------+------------+-----+---+
|    101|    Big Boss|    1| 10|
|    101|    Big Boss|    5| 15|
|    101|    Big Boss|   50| 60|
|    102|    Big Boss|    1| 11|
|    102|    Big Boss|   50| 60|
+-------+------------+-----+---+

df_2 = df_1.withColumn("prev_start", coalesce(lag(col("Start"), 1).over(window_1), lit(0))).withColumn("prev_end", coalesce(lag(col("End"), 1).over(window_2), lit(0)))
df_2.show()
+-------+------------+-----+---+----------+--------+
|User_ID|Episode_Name|Start|End|prev_start|prev_end|
+-------+------------+-----+---+----------+--------+
|    101|    Big Boss|    1| 10|         0|       0|
|    101|    Big Boss|    5| 15|         1|      10|
|    101|    Big Boss|   50| 60|         5|      15|
|    102|    Big Boss|    1| 11|         0|       0|
|    102|    Big Boss|   50| 60|         1|      11|
+-------+------------+-----+---+----------+--------+

df_3 = df_2.withColumn("time", when(col("prev_start") == 0, 0).when(col("Start") < col("prev_end"), col("prev_end")).otherwise("Start")).groupBy("User_ID", "Episode_Name").agg(sum(col("End") - col("time")))
df_3.show()

+-------+------------+-----------------+
|User_ID|Episode_Name|sum((End - time))|
+-------+------------+-----------------+
|    101|    Big Boss|               25|
|    102|    Big Boss|               21|
+-------+------------+-----------------+
