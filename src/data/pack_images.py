import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, element_at, split, regexp_replace

# ====================================================
# 1. CẤU HÌNH MÔI TRƯỜNG & SPARK
# ====================================================
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def pack_images_to_parquet():
    print("🚀 Khởi động Spark để đóng gói Container hình ảnh...")
    # Cấp RAM 8GB cho Spark vì việc đọc file Binary tốn khá nhiều bộ nhớ
    spark = SparkSession.builder \
        .appName("HM_Pack_Images_To_Parquet") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # Đường dẫn
    INPUT_DIR = "./data/processed/images_resized"
    OUTPUT_FILE = "./data/processed/images_data.parquet"

    if not os.path.exists(INPUT_DIR):
        print(f"❌ Lỗi: Không tìm thấy thư mục {INPUT_DIR}!")
        sys.exit()

    # ---------------------------------------------------------
    # BƯỚC 1: ĐỌC ẢNH DẠNG BINARY
    # ---------------------------------------------------------
    print(f"📦 Đang nuốt trọn thư mục ảnh vào RAM (Định dạng Binary)...")
    # Tính năng format("binaryFile") của Spark sẽ đọc ảnh thành các chuỗi Byte siêu tốc
    images_df = spark.read.format("binaryFile").load(INPUT_DIR)

    # ---------------------------------------------------------
    # BƯỚC 2: TRÍCH XUẤT CHÌA KHÓA "article_id"
    # ---------------------------------------------------------
    print("🪪 Đang trích xuất article_id từ tên file...")
    # Dữ liệu ban đầu có cột 'path' dạng: file:/C:/.../0663713001.jpg
    # 1. Cắt chuỗi theo dấu '/' và lấy phần tử cuối cùng -> 0663713001.jpg
    images_df = images_df.withColumn("filename", element_at(split(col("path"), "/"), -1))
    
    # 2. Xóa đuôi '.jpg' để lấy đúng 10 số ID -> 0663713001
    images_df = images_df.withColumn("article_id", regexp_replace(col("filename"), "\.jpg$", ""))

    # Chỉ giữ lại ID và nội dung Byte của ảnh
    final_df = images_df.select("article_id", "content")
    
    # In ra 5 dòng để bạn kiểm tra (Cột content sẽ hiện những chuỗi mã hóa dài ngoằng, đó là bình thường!)
    final_df.show(5)

    # ---------------------------------------------------------
    # BƯỚC 3: XUẤT RA PARQUET
    # ---------------------------------------------------------
    print("💾 Đang nén và ghi ra file Parquet (Quá trình này có thể mất vài phút)...")
    final_df.write.mode("overwrite").parquet(OUTPUT_FILE)

    print(f"\n🎉 HOÀN TẤT XUẤT SẮC! 'Container' Parquet đã sẵn sàng tại: {OUTPUT_FILE}")
    print("👉 Bây giờ bạn chỉ việc lấy đúng file này đẩy lên Hadoop là xong!")
    
    spark.stop()

if __name__ == "__main__":
    pack_images_to_parquet()