import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max as spark_max, to_date, dayofweek, month, year, count
)
from pyspark.ml.feature import StringIndexer
from datetime import timedelta

# ====================================================
# 1. CẤU HÌNH MÔI TRƯỜNG & ĐƯỜNG DẪN
# ====================================================
INPUT_PATH  = "./data/raw/"
OUTPUT_PATH = "./data/processed/"

# Cấu hình Java/Hadoop (Giữ nguyên setup máy bạn đã chạy OK)
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = r"C:\hadoop"

# ====================================================
# 2. KHỞI TẠO SPARK (FIX LỖI IP MẠNG ẢO)
# ====================================================
print("🚀 [M1] Bắt đầu xử lý Transactions (Localhost Mode)...")
     
# Cấu hình tối ưu + Ép buộc dùng Localhost
spark = SparkSession.builder \
    .appName("HM_Process_Transactions_M1") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.network.timeout", "600s") \
    .getOrCreate()
        
spark.sparkContext.setLogLevel("ERROR")

try:
    # --- BƯỚC 1: ĐỌC DỮ LIỆU ---
    print("⏳ Đang đọc file transactions_train.csv...")
    transactions = spark.read.csv(INPUT_PATH + "transactions_train.csv", header=True, inferSchema=True)

    # --- BƯỚC 2: LỌC THỜI GIAN (6 THÁNG CUỐI) ---
    print("⏳ Đang lọc dữ liệu 6 tháng gần nhất...")
    transactions = transactions.withColumn("t_dat_date", to_date(col("t_dat"), "yyyy-MM-dd"))
    
    # Tìm ngày cuối cùng trong dữ liệu
    max_date_row = transactions.agg(spark_max("t_dat_date")).collect()[0][0]
    start_date = max_date_row - timedelta(days=180)
    print(f"   -> Dữ liệu từ ngày: {start_date} đến {max_date_row}")

    # *KỸ THUẬT QUAN TRỌNG*: Repartition để chia tải
    trans_recent = transactions.filter(col("t_dat_date") >= start_date).repartition(200)
    trans_recent.cache() # Cache lại để dùng cho nhiều bước bên dưới

    # ====================================================
    # BƯỚC 3: CHIẾN LƯỢC 1 - TẠO LIST "HÀNG HOT" (FALLBACK)
    # (Dùng cho User mới hoặc mua ít < 5 đơn)
    # ====================================================
    print("🛠️ Đang tạo danh sách 'Top 12 Bán Chạy' (cho Cold Start)...")
    last_month_start = max_date_row - timedelta(days=30)
    
    # Tính top 12 món bán chạy nhất trong 30 ngày cuối
    top_items = trans_recent.filter(col("t_dat_date") >= last_month_start) \
        .groupBy("article_id").count() \
        .orderBy(col("count").desc()) \
        .limit(12)
    
    # Lưu ra CSV để sau này ghép vào file kết quả
    top_items.select("article_id").write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH + "top_popular_items.csv")
    print("   ✅ Đã lưu: top_popular_items.csv")

    # ====================================================
    # BƯỚC 4: CHIẾN LƯỢC 2 - CHUẨN BỊ DATA CHO ALS MODEL
    # (Chỉ lấy User mua >= 5 đơn)
    # ====================================================
    print("⏳ Đang lọc User tích cực (>= 5 đơn) cho Model ALS...")
    user_counts = trans_recent.groupBy("customer_id").count().filter(col("count") >= 5)
    
    # Join để chỉ giữ lại giao dịch của user xịn
    trans_filtered = trans_recent.join(user_counts, on="customer_id", how="inner").drop("count")
    
    # --- MÃ HÓA ID (String -> Int) ---
    print("⏳ Đang mã hóa ID (StringIndexer)...")
    indexer_user = StringIndexer(inputCol="customer_id", outputCol="user_id_int").fit(trans_filtered)
    indexer_item = StringIndexer(inputCol="article_id", outputCol="item_id_int").fit(trans_filtered)
    
    trans_final = indexer_item.transform(indexer_user.transform(trans_filtered))

    # --- LƯU BẢNG MAPPING (Để sau này convert ngược lại khi nộp bài) ---
    print("💾 Đang lưu bảng Mapping (Int -> String)...")
    # Lưu User Mapping
    indexer_user_model = indexer_user
    user_labels = indexer_user_model.labels
    spark.createDataFrame([(i, str(label)) for i, label in enumerate(user_labels)], ["user_id_int", "customer_id"]) \
        .write.mode("overwrite").parquet(OUTPUT_PATH + "user_mapping.parquet")
    
    # Lưu Item Mapping
    indexer_item_model = indexer_item
    item_labels = indexer_item_model.labels
    spark.createDataFrame([(i, str(label)) for i, label in enumerate(item_labels)], ["item_id_int", "article_id"]) \
        .write.mode("overwrite").parquet(OUTPUT_PATH + "item_mapping.parquet")

    # ====================================================
    # BƯỚC 5: CHIA TẬP TRAIN / VALIDATION (QUAN TRỌNG)
    # ====================================================
    print("✂️ Đang chia dữ liệu Train/Validation (Hold-out 7 ngày cuối)...")
    
    # Ngày cắt là 7 ngày trước ngày cuối cùng
    split_date = max_date_row - timedelta(days=7)
    
    # Tập Train: Dữ liệu trước tuần cuối
    train_df = trans_final.filter(col("t_dat_date") < split_date)
    
    # Tập Validation: Dữ liệu tuần cuối cùng (Để test xem model có đoán trúng không)
    val_df = trans_final.filter(col("t_dat_date") >= split_date)
    
    print(f"   -> Train size: {train_df.count():,}")
    print(f"   -> Val size:   {val_df.count():,}")

    # ====================================================
    # BƯỚC 6: XUẤT FILE KẾT QUẢ CHO TEAM
    # ====================================================
    print("💾 Đang lưu các file kết quả cuối cùng...")

    # 1. Lưu Train Set (Cho M3 train ALS)
    train_df.select("user_id_int", "item_id_int") \
        .write.mode("overwrite").parquet(OUTPUT_PATH + "transactions_train_set.parquet")
        
    # 2. Lưu Validation Set (Cho M3 chấm điểm)
    val_df.select("user_id_int", "item_id_int") \
        .write.mode("overwrite").parquet(OUTPUT_PATH + "transactions_val_set.parquet")

    # 3. Xuất danh sách User hợp lệ (Cho M2 làm file Customers)
    print("   -> Đang xuất valid_users.csv cho M2...")
    trans_filtered.select("customer_id").distinct() \
        .write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH + "valid_users.csv")

    # 4. Xuất danh sách Item hợp lệ (Cho M4 làm file Articles)
    print("   -> Đang xuất valid_items.csv cho M4...")
    trans_filtered.select("article_id").distinct() \
        .write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH + "valid_items.csv")

    print("\n🎉 M1 ĐÃ HOÀN THÀNH NHIỆM VỤ XUẤT SẮC!")
    print("📂 Kiểm tra thư mục data/processed/ để thấy các file vừa tạo.")

except Exception as e:
    print(f"\n❌ CÓ LỖI XẢY RA: {str(e)}")

finally:
    spark.stop()