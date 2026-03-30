import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# --- CẤU HÌNH LOCAL ---
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

INPUT_PATH  = "./data/raw/"
OUTPUT_PATH = "./data/processed/"

spark = SparkSession.builder \
    .appName("HM_Process_Customers_Local") \
    .master("local[*]") \
    .config("spark.driver.memory", "5g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    print("⏳ [M1] Đang xử lý Customers...")
    # Đọc dữ liệu gốc
    customers = spark.read.csv(INPUT_PATH + "customers.csv", header=True, inferSchema=True)

    # 1. Điền giá trị thiếu cho các cột trạng thái
    customers = customers.fillna({"FN": 0.0, "Active": 0.0})

    # 2. Xử lý Tuổi và Chia nhóm tuổi (Cho kỹ thuật ngoài giáo trình)
    # Tính tuổi trung vị để điền vào các ô trống
    median_age = customers.approxQuantile("age", [0.5], 0.01)[0]
    customers = customers.fillna({"age": int(median_age)})

    customers = customers.withColumn("age_group", 
        when(col("age") < 20, "Teen")
        .when(col("age") < 35, "Young Adult")
        .when(col("age") < 55, "Adult")
        .otherwise("Senior"))

    # 3. Số hóa các trạng thái thành chỉ mục (Index) để tối ưu dung lượng
    customers = customers.withColumn("club_status_index",
        when(col("club_member_status") == "ACTIVE", 0)
        .when(col("club_member_status") == "PRE-CREATE", 1)
        .otherwise(2))

    # Lưu ra Parquet
    customers.select("customer_id", "age", "age_group", "FN", "Active", "club_status_index") \
             .write.mode("overwrite").parquet(OUTPUT_PATH + "customers_processed.parquet")

    print(f"✅ Xong Customers! Đã chia nhóm tuổi và điền tuổi trung vị: {int(median_age)}")

finally:
    spark.stop()