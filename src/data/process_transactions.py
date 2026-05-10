import os
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

INPUT_PATH  = "./data/raw/"
OUTPUT_PATH = "./data/processed/"

spark = SparkSession.builder \
    .appName("HM_Process_Transactions") \
    .master("local[*]") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    print("Processing Transactions...")
    df = spark.read.csv(INPUT_PATH + "transactions_train.csv", header=True, inferSchema=True)
    
    df = df.withColumn("t_dat_date", F.to_date(F.col("t_dat"), "yyyy-MM-dd"))
    df = df.withColumn("article_id", F.lpad(F.col("article_id").cast("string"), 10, "0"))

    max_date = df.select(F.max("t_dat_date")).collect()[0][0]
    
    test_start  = max_date - datetime.timedelta(days=7)
    val_start   = test_start - datetime.timedelta(days=7)
    train_start = val_start - datetime.timedelta(weeks=6)

    print(f"Dataset Max Date: {max_date}")
    print(f"Retrieval Train (W1-6): {train_start} to {val_start}")
    print(f"Ranking Train (W7):     {val_start} to {test_start}")
    print(f"Final Test (W8):        {test_start} to {max_date}")

    windowSpec = Window.partitionBy("customer_id").orderBy(F.desc("t_dat_date"))
    df_ranked = df.withColumn("purchase_rank", F.row_number().over(windowSpec))
    df_ranked.write.mode("overwrite").parquet(OUTPUT_PATH + "cleaned_transactions.parquet")

    train_6w_df = df.filter((F.col("t_dat_date") >= train_start) & (F.col("t_dat_date") < val_start))
    
    baskets = train_6w_df.groupBy("customer_id", "t_dat_date") \
                         .agg(F.collect_set("article_id").alias("items")) \
                         .filter(F.size(F.col("items")) > 1)

    baskets.write.mode("overwrite").parquet(OUTPUT_PATH + "train_baskets_session.parquet")
    print("Transactions processing completed successfully.")

finally:
    spark.stop()