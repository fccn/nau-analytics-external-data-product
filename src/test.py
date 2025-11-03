from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.range(1, 101)
print(f"Count: {df.count()}")
spark.stop()
