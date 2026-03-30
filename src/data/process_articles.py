import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, when

# --- CẤU HÌNH LOCAL ---
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

INPUT_PATH  = "./data/raw/"
OUTPUT_PATH = "./data/processed/"

spark = SparkSession.builder \
    .appName("HM_Process_Articles_Local") \
    .master("local[*]") \
    .config("spark.driver.memory", "5g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    print("⏳ [M1] Đang xử lý Articles...")
    # Đọc dữ liệu gốc
    articles = spark.read.csv(INPUT_PATH + "articles.csv", header=True, inferSchema=True)

    # KỸ THUẬT QUAN TRỌNG: Thêm số 0 ở đầu để đủ 10 ký tự (Khớp với file ảnh .jpg)
    articles = articles.withColumn("article_id", lpad(col("article_id").cast("string"), 10, "0"))

    # KỸ THUẬT NÂNG CAO: Suy luận giới tính để lọc gợi ý (Hybrid Filtering)
    articles = articles.withColumn("gender", 
        when(col("index_group_name").contains("Ladies"), "Female")
        .when(col("index_group_name").contains("Men"), "Male")
        .otherwise("Unisex"))

    # Xử lý giá trị thiếu cho các cột mô tả
    cols_to_fill = ["prod_name", "product_type_name", "product_group_name", "colour_group_name", "detail_desc"]
    articles = articles.na.fill({c: "Unknown" for c in cols_to_fill})

    # Lưu ra Parquet để truy vấn siêu tốc
    articles.select("article_id", "prod_name", "product_type_name", "product_group_name", "colour_group_name", "gender", "detail_desc") \
            .write.mode("overwrite").parquet(OUTPUT_PATH + "articles_processed.parquet")

    print("✅ Xong Articles! ID đã được chuẩn hóa 10 ký tự và thêm cột Gender.")

finally:
    spark.stop()