import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, when, substring

os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

INPUT_PATH  = "./data/raw/"
OUTPUT_PATH = "./data/processed/"

spark = SparkSession.builder \
    .appName("HM_Process_Articles") \
    .master("local[*]") \
    .config("spark.driver.memory", "5g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    print("Processing Articles...")
    articles = spark.read.csv(INPUT_PATH + "articles.csv", header=True, inferSchema=True)

    # Pad article_id and extract product_code for retrieval strategies
    articles = articles.withColumn("article_id", lpad(col("article_id").cast("string"), 10, "0"))
    articles = articles.withColumn("product_code", substring(col("article_id"), 1, 6))

    articles = articles.withColumn("gender", 
        when(col("index_group_name").contains("Ladies"), "Female")
        .when(col("index_group_name").contains("Men"), "Male")
        .otherwise("Unisex"))

    cols_to_fill = ["prod_name", "product_type_name", "product_group_name", "colour_group_name", "detail_desc"]
    articles = articles.na.fill({c: "Unknown" for c in cols_to_fill})

    final_cols = ["article_id", "product_code", "prod_name", "product_type_name", 
                  "product_group_name", "colour_group_name", "gender", "detail_desc"]
                  
    articles.select(final_cols).write.mode("overwrite").parquet(OUTPUT_PATH + "articles_processed.parquet")
    print("Articles processing completed successfully.")

finally:
    spark.stop()