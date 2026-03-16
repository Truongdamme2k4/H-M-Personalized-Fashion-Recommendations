import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# ====================================================
# 1. CẤU HÌNH MÔI TRƯỜNG & SPARK (SAFE MODE)
# ====================================================
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def process_customers_data(spark, input_path, trans_path, output_path):
    # ---------------------------------------------------------
    # BƯỚC 1: ĐỌC DỮ LIỆU
    # ---------------------------------------------------------
    print(f"1️⃣ Đang đọc Customers từ: {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True).dropDuplicates(["customer_id"])

    # ---------------------------------------------------------
    # BƯỚC 2: LỌC KHÁCH HÀNG TỪ TRANSACTIONS (MỚI)
    # ---------------------------------------------------------
    print(f"2️⃣ Đang lọc khách hàng có giao dịch từ: {trans_path}")
    if not os.path.exists(trans_path):
        print("❌ LỖI: Không tìm thấy cleaned_transactions.parquet! Hãy chạy file tiền xử lý Transactions trước.")
        sys.exit()
        
    # Chỉ lấy những ID khách hàng đã thực sự mua đồ (để giảm dung lượng)
    df_trans_users = spark.read.parquet(trans_path).select("customer_id").distinct()
    
    # Inner Join: Giữ lại khách hàng hợp lệ
    df_joined = df.join(df_trans_users, on="customer_id", how="inner")
    
    print("✅ Đã lọc xong khách hàng hợp lệ. Bắt đầu làm sạch...")

    # ---------------------------------------------------------
    # BƯỚC 3: XỬ LÝ DỮ LIỆU CHI TIẾT
    # ---------------------------------------------------------
    
    # 3.1. FN và Active: FillNA = 0
    df_cleaned = df_joined.fillna({"FN": 0, "Active": 0})

    # 3.2. Club Member Status -> Số hóa
    df_cleaned = df_cleaned.withColumn("club_member_status", F.upper(F.trim(F.col("club_member_status"))))
    df_cleaned = df_cleaned.withColumn(
        "club_status_index",
        F.when(F.col("club_member_status") == "ACTIVE", 0)
         .when(F.col("club_member_status") == "PRE-CREATE", 1)
         .when(F.col("club_member_status") == "LEFT CLUB", 2)
         .otherwise(-1) # Unknown
    )

    # 3.3. Fashion News Frequency -> Số hóa
    df_cleaned = df_cleaned.withColumn("fashion_news_frequency", F.upper(F.trim(F.col("fashion_news_frequency"))))
    df_cleaned = df_cleaned.withColumn(
        "news_freq_index",
        F.when(F.col("fashion_news_frequency").isin("NONE", "NO"), 0)
         .when(F.col("fashion_news_frequency") == "MONTHLY", 1)
         .when(F.col("fashion_news_frequency") == "REGULARLY", 2)
         .otherwise(0) # Mặc định là None
    )

    # 3.4. Xử lý Tuổi (Age)
    df_cleaned = df_cleaned.withColumn("age", F.col("age").cast(IntegerType()))
    
    # Lọc nhiễu: Tuổi < 15 hoặc > 100 thì cho thành Null
    df_cleaned = df_cleaned.withColumn(
        "age", 
        F.when((F.col("age") < 15) | (F.col("age") > 100), None).otherwise(F.col("age"))
    )

    # Điền tuổi thiếu bằng Median toàn cục
    median_age = df_cleaned.approxQuantile("age", [0.5], 0.01)[0]
    print(f"ℹ️ Tuổi trung vị (Median Age) dùng để fill: {int(median_age)}")
    
    df_cleaned = df_cleaned.fillna({"age": int(median_age)})

    # ---------------------------------------------------------
    # BƯỚC 4: LỌC CỘT VÀ LƯU TRỮ
    # ---------------------------------------------------------
    print("4️⃣ Đang lưu file chuẩn...")
    
    # SỬA CHỮA QUAN TRỌNG: Giữ nguyên customer_id (Dạng String)
    final_cols = [
        "customer_id",      # Key kết nối
        "age",              # Để filter độ tuổi (GenZ, Millenial)
        "FN", 
        "Active", 
        "club_status_index",
        "news_freq_index" 
    ]
    
    df_final = df_cleaned.select(final_cols)
    df_final.show(5)
    
    # Lưu Parquet
    df_final.write.mode("overwrite").parquet(output_path)
    print(f"🎉 Hoàn tất! File Customers chuẩn đã lưu tại: {output_path}")

if __name__ == "__main__":
    print("🚀 Đang khởi động Spark (Safe Mode 5GB)...")
    spark = SparkSession.builder \
        .appName("HM_Process_Customers_Multimodal") \
        .master("local[*]") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    INPUT_FILE = "./data/raw/customers.csv"
    # Lấy danh sách giao dịch vừa chạy xong ở file trước
    TRANS_FILE = "./data/processed/cleaned_transactions.parquet"
    OUTPUT_FILE = "./data/processed/customers_processed.parquet"
    
    if os.path.exists(INPUT_FILE):
        process_customers_data(spark, INPUT_FILE, TRANS_FILE, OUTPUT_FILE)
    else:
        print(f"❌ Không tìm thấy file: {INPUT_FILE}")
        
    spark.stop()