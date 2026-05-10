from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Cluster_Process_Articles_Multimodal") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

data = ["hello world", "hello spark", "spark is great", "hello world spark"]
rdd = spark.sparkContext.parallelize(data)

counts = rdd.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

df = counts.toDF(["word", "count"])
df.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/output/wordcount", header=True)

for word, count in counts.collect():
    print(f"{word}: {count}")

spark.stop()