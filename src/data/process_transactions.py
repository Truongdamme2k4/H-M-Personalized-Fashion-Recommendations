import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lpad, collect_list, size

# 1. CẤU HÌNH MÔI TRƯỜNG
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = r"C:\hadoop"

INPUT_PATH  = "./data/raw/"
OUTPUT_PATH = "./data/processed/"

print("🚀 [M1] Bắt đầu tiền xử lý FULL 31.7 triệu giao dịch H&M...")

# 2. KHỞI TẠO SPARK
spark = SparkSession.builder \
    .appName("HM_Process_Full_Transactions") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    # --- BƯỚC 1: ĐỌC FULL DỮ LIỆU ---
    print("⏳ Đang đọc toàn bộ file transactions_train.csv...")
    transactions = spark.read.csv(INPUT_PATH + "transactions_train.csv", header=True, inferSchema=True)

    # --- BƯỚC 2: CHUẨN HÓA DỮ LIỆU ---
    print("⏳ Đang chuẩn hóa định dạng ngày tháng và ID sản phẩm...")
    # Ép kiểu ngày tháng
    transactions = transactions.withColumn("t_dat_date", to_date(col("t_dat"), "yyyy-MM-dd"))
    
    # KỸ THUẬT QUAN TRỌNG: Thêm số 0 vào đầu article_id để sau này khớp với tên file ảnh (VD: 0663713001.jpg)
    transactions = transactions.withColumn("article_id", lpad(col("article_id").cast("string"), 10, "0"))

    # Repartition để chia đều tải ra các core của CPU
    trans_recent = transactions.repartition(200)
    trans_recent.cache()

    print(f"✅ Tổng số giao dịch đang xử lý: {trans_recent.count():,} dòng")

    # --- BƯỚC 3: TẠO GIỎ HÀNG CHO FP-GROWTH ---
    print("🛒 Đang gom nhóm tạo Giỏ hàng (Market Baskets)...")
    # Gom những món đồ được mua bởi CÙNG 1 người trong CÙNG 1 ngày thành 1 giỏ
    baskets_df = trans_recent.groupBy("customer_id", "t_dat_date") \
        .agg(collect_list("article_id").alias("items")) \
        .filter(size(col("items")) > 1) # Chỉ lấy giỏ hàng có từ 2 món trở lên để tìm luật mua kèm

    # Lưu ra Parquet (Siêu nhẹ, siêu nhanh)
    baskets_df.write.mode("overwrite").parquet(OUTPUT_PATH + "market_baskets.parquet")
    print("   -> Đã lưu: market_baskets.parquet")

    # --- BƯỚC 4: LƯU BẢN CLEAN CỦA TOÀN BỘ TRANSACTIONS ---
    print("💾 Đang lưu bản Clean của Transactions để dùng cho phân tích Trend...")
    trans_recent.select("t_dat_date", "customer_id", "article_id", "price") \
        .write.mode("overwrite").parquet(OUTPUT_PATH + "cleaned_transactions.parquet")
    
    # Xuất danh sách Item hợp lệ
    trans_recent.select("article_id").distinct() \
        .write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH + "valid_articles.csv")

    print("\n🎉 XONG! DỮ LIỆU ĐÃ SẴN SÀNG CHO BƯỚC KHAI PHÁ.")

except Exception as e:
    print(f"\n❌ CÓ LỖI XẢY RA: {str(e)}")

finally:
    spark.stop()