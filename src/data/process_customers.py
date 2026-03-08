import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType

# ====================================================
# 1. CẤU HÌNH MÔI TRƯỜNG & SPARK (SAFE MODE)
# ====================================================
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def process_customers_data(spark, input_path, mapping_path, output_path):
    # ---------------------------------------------------------
    # BƯỚC 1: ĐỌC DỮ LIỆU
    # ---------------------------------------------------------
    print(f"1️⃣ Đang đọc Customers từ: {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Xử lý ID trùng lặp (nếu có)
    df = df.dropDuplicates(["customer_id"])

    # ---------------------------------------------------------
    # BƯỚC 2: JOIN VỚI MAPPING ĐỂ LẤY ID CHUẨN (Quan trọng nhất)
    # ---------------------------------------------------------
    print(f"2️⃣ Đang đọc Mapping từ: {mapping_path}")
    if not os.path.exists(mapping_path):
        print("❌ LỖI: Không tìm thấy file user_mapping.parquet!")
        sys.exit()
        
    df_map = spark.read.parquet(mapping_path)
    
    # Inner Join: Chỉ giữ lại khách hàng ĐÃ TỪNG MUA HÀNG
    # (Giúp loại bỏ khách hàng rác, giảm kích thước file)
    df_joined = df.join(df_map, on="customer_id", how="inner")
    
    print("✅ Đã Map ID thành công. Bắt đầu làm sạch...")

    # ---------------------------------------------------------
    # BƯỚC 3: XỬ LÝ DỮ LIỆU (Logic giống hệt Pandas)
    # ---------------------------------------------------------
    
    # 3.1. FN và Active: FillNA = 0
    df_cleaned = df_joined.fillna({"FN": 0, "Active": 0})

    # 3.2. Club Member Status -> Số hóa
    # Logic: ACTIVE=0, PRE-CREATE=1, LEFT CLUB=2, UNKNOWN=-1
    # Bước này dùng hàm when-otherwise (giống if-else)
    
    # Chuẩn hóa text trước (Trim + Upper)
    df_cleaned = df_cleaned.withColumn("club_member_status", F.upper(F.trim(F.col("club_member_status"))))
    
    df_cleaned = df_cleaned.withColumn(
        "club_status_index",
        F.when(F.col("club_member_status") == "ACTIVE", 0)
         .when(F.col("club_member_status") == "PRE-CREATE", 1)
         .when(F.col("club_member_status") == "LEFT CLUB", 2)
         .otherwise(-1) # Unknown
    )

    # 3.3. Fashion News Frequency -> Số hóa
    # Logic: None=0, Monthly=1, Regularly=2
    df_cleaned = df_cleaned.withColumn("fashion_news_frequency", F.upper(F.trim(F.col("fashion_news_frequency"))))
    
    df_cleaned = df_cleaned.withColumn(
        "news_freq_index",
        F.when(F.col("fashion_news_frequency").isin("NONE", "NO"), 0)
         .when(F.col("fashion_news_frequency") == "MONTHLY", 1)
         .when(F.col("fashion_news_frequency") == "REGULARLY", 2)
         .otherwise(0) # Mặc định là None
    )

    # 3.4. Xử lý Tuổi (Age)
    # Cast sang số nguyên
    df_cleaned = df_cleaned.withColumn("age", F.col("age").cast(IntegerType()))
    
    # Lọc nhiễu: Tuổi < 15 hoặc > 100 thì cho thành Null để điền lại
    df_cleaned = df_cleaned.withColumn(
        "age", 
        F.when((F.col("age") < 15) | (F.col("age") > 100), None).otherwise(F.col("age"))
    )

    # Điền tuổi thiếu:
    # Cách đơn giản & hiệu quả nhất cho Spark: Điền bằng Median toàn cục (Global Median)
    # (Dùng Group Median trong Spark khá phức tạp và tốn RAM, Global Median là đủ tốt cho model rồi)
    median_age = df_cleaned.approxQuantile("age", [0.5], 0.01)[0]
    print(f"ℹ️ Tuổi trung vị (Median Age) là: {median_age}")
    
    df_cleaned = df_cleaned.fillna({"age": int(median_age)})

    # ---------------------------------------------------------
    # BƯỚC 4: LỌC CỘT VÀ LƯU TRỮ
    # ---------------------------------------------------------
    print("4️⃣ Đang lưu file chuẩn...")
    
    # Chỉ chọn các cột số cần thiết cho Model
    final_cols = [
        "user_id_int",      # ID chuẩn (Key)
        "age",              # Feature 1
        "FN",               # Feature 2
        "Active",           # Feature 3
        "club_status_index",# Feature 4 (Đã mã hóa)
        "news_freq_index"   # Feature 5 (Đã mã hóa)
    ]
    
    df_final = df_cleaned.select(final_cols)
    
    # In kiểm tra
    df_final.show(10)
    
    # Lưu Parquet
    df_final.write.mode("overwrite").parquet(output_path)
    print(f"🎉 Hoàn tất! File Customers chuẩn đã lưu tại: {output_path}")

if __name__ == "__main__":
    print("🚀 Đang khởi động Spark (Safe Mode 16GB)...")
    spark = SparkSession.builder \
        .appName("HM_Process_Customers_Spark") \
        .master("local[*]") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
    
    # Đường dẫn (Tự động nhận diện thư mục)
    # Lưu ý: Chỉnh lại path nếu cấu trúc folder của bạn khác
    INPUT_FILE = "./data/raw/customers.csv"
    MAPPING_FILE = "./data/processed/user_mapping.parquet"
    OUTPUT_FILE = "./data/processed/customers_processed.parquet"
    
    if os.path.exists(INPUT_FILE):
        process_customers_data(spark, INPUT_FILE, MAPPING_FILE, OUTPUT_FILE)
    else:
        print(f"❌ Không tìm thấy file: {INPUT_FILE}")
        
    spark.stop()