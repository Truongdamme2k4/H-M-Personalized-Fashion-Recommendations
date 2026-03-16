import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# ====================================================
# 1. CẤU HÌNH MÔI TRƯỜNG & SPARK (SAFE MODE)
# ====================================================
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def process_articles_data(spark, input_path, valid_items_path, output_path):
    print(f"1️⃣ Đang đọc file gốc: {input_path}")
    df_articles = spark.read.csv(input_path, header=True, inferSchema=True)

    # ---------------------------------------------------------
    # BƯỚC 1: XỬ LÝ ID (CỰC KỲ QUAN TRỌNG ĐỂ NỐI VỚI ẢNH)
    # ---------------------------------------------------------
    # Thêm số 0 vào trước ID để đủ 10 ký tự (Giống hệt tên file .jpg)
    df_articles = df_articles.withColumn("article_id", F.lpad(F.col("article_id").cast("string"), 10, "0"))

    # ---------------------------------------------------------
    # BƯỚC 2: LỌC SẢN PHẨM TỪ GIAO DỊCH (Bỏ hàng tồn kho không ai mua)
    # ---------------------------------------------------------
    print(f"2️⃣ Đang lọc sản phẩm hợp lệ từ: {valid_items_path}")
    if not os.path.exists(valid_items_path):
        print("❌ LỖI: Không tìm thấy file valid_articles.csv! Hãy chạy file process_transactions.py trước.")
        sys.exit()
        
    df_valid = spark.read.csv(valid_items_path, header=True)
    
    # Đảm bảo file valid cũng được lpad để join cho chuẩn
    df_valid = df_valid.withColumn("article_id", F.lpad(F.col("article_id").cast("string"), 10, "0"))
    
    # Inner Join: Chỉ giữ lại các sản phẩm đã từng được bán
    df_joined = df_articles.join(df_valid, on="article_id", how="inner")

    # ---------------------------------------------------------
    # BƯỚC 3: XỬ LÝ MISSING VALUES
    # ---------------------------------------------------------
    print("3️⃣ Đang xử lý missing values...")
    cols_to_fill = [
        "prod_name", 
        "product_type_name", 
        "product_group_name", 
        "colour_group_name", 
        "department_name", 
        "detail_desc"
    ]
    df_cleaned = df_joined.fillna({c: "Unknown" for c in cols_to_fill})

    # ---------------------------------------------------------
    # BƯỚC 4: LỌC CỘT VÀ LƯU TRỮ (GIỮ LẠI ĐỂ LÀM DASHBOARD)
    # ---------------------------------------------------------
    print("4️⃣ Đang lưu kết quả...")
    final_cols = [
        "article_id",         # Key kết nối (Dạng String 10 ký tự)
        "prod_name",          # Tên sp
        "product_type_name",  # Loại (T-shirt, Trousers...)
        "product_group_name", # Nhóm hàng
        "colour_group_name",  # Màu sắc (Dùng để bắt Trend)
        "detail_desc"         # Mô tả chi tiết (In ra giao diện UI)
    ]
    
    df_final = df_cleaned.select(final_cols)
    
    # Hiển thị 5 dòng để check
    df_final.show(5, truncate=False)

    # Lưu ra Parquet
    df_final.write.mode("overwrite").parquet(output_path)
    print(f"🎉 Hoàn tất! File Articles chuẩn đã lưu tại: {output_path}")

if __name__ == "__main__":
    print("🚀 Đang khởi động Spark (Safe Mode 5GB)...")
    spark = SparkSession.builder \
        .appName("HM_Process_Articles_Multimodal") \
        .master("local[*]") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
        
    # ĐƯỜNG DẪN FILE
    INPUT_FILE = "./data/raw/articles.csv" 
    # File lấy từ kết quả chạy file Transactions
    VALID_ITEMS_FILE = "./data/processed/valid_articles.csv" 
    OUTPUT_FILE = "./data/processed/articles_processed.parquet"
    
    if os.path.exists(INPUT_FILE):
        process_articles_data(spark, INPUT_FILE, VALID_ITEMS_FILE, OUTPUT_FILE)
    else:
        print(f"❌ Không tìm thấy file gốc: {INPUT_FILE}")
    
    spark.stop()