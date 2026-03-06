import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max as spark_max, to_date, dayofweek, month
)
from pyspark.ml.feature import StringIndexer
from datetime import timedelta

# ====================================================
# 1. CẤU HÌNH MÔI TRƯỜNG & ĐƯỜNG DẪN
# ====================================================
# Đường dẫn (Sửa lại nếu cần)
INPUT_PATH  = "./data/raw/"
OUTPUT_PATH = "./data/processed/"

# Cấu hình Java/Hadoop (Giữ nguyên setup máy bạn đã chạy ok)
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = r"C:\hadoop"

# ====================================================
# 2. KHỞI TẠO SPARK (CẤU HÌNH TỐI ƯU CHO FILE NẶNG)
# ====================================================
print("🚀 [M1] Bắt đầu xử lý Transactions...")
spark = SparkSession.builder \
    .appName("HM_Process_Transactions_M1") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.driver.maxResultSize", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ====================================================
# 3. QUY TRÌNH XỬ LÝ
# ====================================================
try:
    # --- BƯỚC 1: Đọc dữ liệu thô ---
    print("⏳ Đang đọc file transactions_train.csv...")
    transactions = spark.read.csv(INPUT_PATH + "transactions_train.csv", header=True, inferSchema=True)

    # --- BƯỚC 2: Lọc thời gian (6 tháng cuối) ---
    print("⏳ Đang lọc dữ liệu 6 tháng gần nhất...")
    transactions = transactions.withColumn("t_dat_date", to_date(col("t_dat"), "yyyy-MM-dd"))
    
    # Tìm ngày mua cuối cùng trong data
    max_date_row = transactions.agg(spark_max("t_dat_date")).collect()[0][0]
    start_date = max_date_row - timedelta(days=180)
    print(f"   -> Lấy dữ liệu từ ngày: {start_date}")

    # *KỸ THUẬT QUAN TRỌNG*: Repartition ngay sau khi filter để chia tải RAM
    trans_recent = transactions.filter(col("t_dat_date") >= start_date).repartition(200)

    # --- BƯỚC 3: Lọc User rác (Cold Start) ---
    # Chỉ giữ lại user mua >= 5 đơn hàng để Model học cho chuẩn
    print("⏳ Đang loại bỏ các user mua ít (< 5 đơn)...")
    user_counts = trans_recent.groupBy("customer_id").count().filter(col("count") >= 5)
    
    # Join ngược lại để lọc bảng chính
    trans_filtered = trans_recent.join(user_counts, on="customer_id", how="inner").drop("count")
    
    # Cache lại vì biến này sẽ dùng nhiều lần bên dưới
    trans_filtered.cache()
    count_rows = trans_filtered.count()
    print(f"   -> Số lượng giao dịch còn lại sau lọc: {count_rows:,}")

    # --- BƯỚC 4: Mã hóa ID (StringIndexer) ---
    # Chuyển ID dài ngoằng (String) thành số nguyên (Int) để ALS chạy nhanh gấp 10 lần
    print("⏳ Đang mã hóa ID (String -> Int)...")
    
    indexer_user = StringIndexer(inputCol="customer_id", outputCol="user_id_int").fit(trans_filtered)
    indexer_item = StringIndexer(inputCol="article_id", outputCol="item_id_int").fit(trans_filtered)
    
    trans_final = indexer_item.transform(indexer_user.transform(trans_filtered))

    # --- BƯỚC 5: Feature Engineering (Tạo đặc trưng thời gian) ---
    trans_final = trans_final \
        .withColumn("purchase_month", month(col("t_dat_date"))) \
        .withColumn("purchase_dayofweek", dayofweek(col("t_dat_date")))

    # ====================================================
    # 4. XUẤT KẾT QUẢ (QUAN TRỌNG CHO TEAMWORK)
    # ====================================================
    print("💾 Đang lưu dữ liệu...")

    # 1. Lưu bảng Transaction sạch (Cho nhóm Model dùng)
    trans_final.select("t_dat_date", "purchase_month", "purchase_dayofweek", "user_id_int", "item_id_int", "price") \
        .write.mode("overwrite").parquet(OUTPUT_PATH + "transactions_clean.parquet")
    print("   ✅ Đã lưu: transactions_clean.parquet")

    # 2. Xuất danh sách User hợp lệ (Cho bạn làm file Customers)
    print("   -> Đang xuất valid_users.csv cho M2...")
    trans_filtered.select("customer_id").distinct() \
        .write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH + "valid_users.csv")

    # 3. Xuất danh sách Item hợp lệ (Cho bạn làm file Articles)
    print("   -> Đang xuất valid_items.csv cho M4...")
    trans_filtered.select("article_id").distinct() \
        .write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH + "valid_items.csv")

    print("\n🎉 M1 ĐÃ HOÀN THÀNH NHIỆM VỤ!")
    print("👉 Hãy gửi file 'valid_users.csv' cho M2 và 'valid_items.csv' cho M4 nhé!")

except Exception as e:
    print(f"\n❌ CÓ LỖI XẢY RA: {str(e)}")

finally:
    spark.stop()