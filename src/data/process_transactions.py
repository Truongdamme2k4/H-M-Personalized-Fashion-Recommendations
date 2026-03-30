import os
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ====================================================
# 1. CẤU HÌNH MÔI TRƯỜNG LOCAL
# ====================================================
# Lưu ý: Thay đổi đường dẫn Java/Hadoop nếu máy bạn cài ở vị trí khác
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

INPUT_PATH  = "./data/raw/"
OUTPUT_PATH = "./data/processed/"

# Khởi tạo Spark với cấu hình RAM tối ưu cho máy cá nhân
spark = SparkSession.builder \
    .appName("HM_Process_8Weeks_Official") \
    .master("local[*]") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    print("⏳ Bước 1: Đọc và Tiền xử lý dữ liệu thô...")
    # Đọc CSV và ép kiểu dữ liệu
    df = spark.read.csv(INPUT_PATH + "transactions_train.csv", header=True, inferSchema=True)
    
    # Chuẩn hóa ngày tháng và Article ID (giữ đủ 10 chữ số)
    df = df.withColumn("t_dat_date", F.to_date(F.col("t_dat"), "yyyy-MM-dd"))
    df = df.withColumn("article_id", F.lpad(F.col("article_id").cast("string"), 10, "0"))

    # ---------------------------------------------------------
    # BƯỚC 2: XÁC ĐỊNH CÁC MỐC THỜI GIAN (LỘ TRÌNH 8 TUẦN)
    # ---------------------------------------------------------
    # Sử dụng F.max() để tránh lỗi Java CodeGen trên Windows
    max_date = df.select(F.max("t_dat_date")).collect()[0][0]
    
    # Chia mốc thời gian: 6 tuần Train -> 1 tuần Val -> 1 tuần Test
    test_start = max_date - datetime.timedelta(days=7)       # Tuần 8 (Test/Demo)
    val_start  = test_start - datetime.timedelta(days=7)      # Tuần 7 (Validation/Ranking Train)
    train_start = val_start - datetime.timedelta(weeks=6)    # Tuần 1-6 (Retrieval Train)

    print("-" * 50)
    print(f"📅 Mốc thời gian mới nhất trong dữ liệu: {max_date}")
    print(f"🚀 Giai đoạn 1 (Train Retrieval - W1-6): {train_start} -> {val_start}")
    print(f"🚀 Giai đoạn 2 (Train Ranking - W7):    {val_start} -> {test_start}")
    print(f"🚀 Giai đoạn 3 (Web Demo - W8):         {test_start} -> {max_date}")
    print("-" * 50)

    # ---------------------------------------------------------
    # BƯỚC 3: TẠO CLEANED TRANSACTIONS (Phục vụ ALS & Ranking)
    # ---------------------------------------------------------
    print("💾 Đang lưu Cleaned Transactions (Full History)...")
    
    # Tạo Purchase Rank (Món mới mua nhất sẽ có rank = 1)
    windowSpec = Window.partitionBy("customer_id").orderBy(F.desc("t_dat_date"))
    df_ranked = df.withColumn("purchase_rank", F.row_number().over(windowSpec))
    
    # Lưu file Parquet chính
    df_ranked.write.mode("overwrite").parquet(OUTPUT_PATH + "cleaned_transactions.parquet")

    # ---------------------------------------------------------
    # BƯỚC 4: TẠO GIỎ HÀNG THEO PHIÊN (Phục vụ Member 2 - FP-Growth)
    # ---------------------------------------------------------
    print("📦 Đang tạo Giỏ hàng theo phiên (Chỉ dùng W1-6)...")
    
    # 1. Lọc lấy dữ liệu 6 tuần đầu để làm luật kết hợp
    train_6w_df = df.filter((F.col("t_dat_date") >= train_start) & (F.col("t_dat_date") < val_start))
    
    # 2. Gom nhóm theo [Khách hàng + Ngày] để tính là một phiên mua sắm (Transaction)
    # Dùng collect_set để loại bỏ các món mua trùng trong cùng 1 bill
    baskets = train_6w_df.groupBy("customer_id", "t_dat_date") \
                         .agg(F.collect_set("article_id").alias("items")) \
                         .filter(F.size(F.col("items")) > 1) # Chỉ lấy đơn hàng có từ 2 món trở lên

    # 3. Lưu sản phẩm bàn giao cho Member 2
    baskets.write.mode("overwrite").parquet(OUTPUT_PATH + "train_baskets_session.parquet")

    print(f"✅ Hoàn tất xuất sắc!")
    print(f"📁 Đầu ra 1: {OUTPUT_PATH}cleaned_transactions.parquet")
    print(f"📁 Đầu ra 2: {OUTPUT_PATH}train_baskets_session.parquet")
    print("-" * 50)

finally:
    spark.stop()
    print("🔌 Spark Session đã đóng.")