from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

df_read = spark.read.csv("hdfs://namenode:9000/output/wordcount", header=True, inferSchema=True)

print("=== Doc lai file tu HDFS ===")
df_read.show(truncate=False)

print(f"Tong so dong: {df_read.count()}")
print(f"Schema:")
df_read.printSchema()

spark.stop()