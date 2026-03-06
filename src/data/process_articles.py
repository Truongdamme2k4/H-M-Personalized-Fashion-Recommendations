import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml import Pipeline

# ====================================================
# 1. CẤU HÌNH MÔI TRƯỜNG & SPARK (SAFE MODE)
# ====================================================
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jre1.8.0_421"
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def process_articles_data(spark, input_path, mapping_path, output_path):
    print(f"1️⃣ Đang đọc file gốc: {input_path}")
    df_articles = spark.read.csv(input_path, header=True, inferSchema=True)

    # Chuyển đổi article_id sang String để đảm bảo việc Join không bị lỗi type
    df_articles = df_articles.withColumn("article_id", F.col("article_id").cast("string"))

    # ---------------------------------------------------------
    # BƯỚC QUAN TRỌNG: ĐỌC MAPPING TỪ M1 ĐỂ LẤY ID CHUẨN
    # ---------------------------------------------------------
    print(f"2️⃣ Đang đọc file Mapping: {mapping_path}")
    if not os.path.exists(mapping_path):
        print("❌ LỖI: Không tìm thấy file item_mapping.parquet!")
        print("👉 Hãy chạy process_transactions.py của M1 trước.")
        sys.exit()
        
    df_mapping = spark.read.parquet(mapping_path)
    # Mapping cũng phải đảm bảo article_id là string
    df_mapping = df_mapping.withColumn("article_id", F.col("article_id").cast("string"))

    # 3. Xử lý Missing Values
    print("3️⃣ Đang xử lý missing values...")
    df_articles = df_articles.fillna({
        "detail_desc": "", 
        "prod_name": "",
        "product_type_name": "",
        "colour_group_name": ""
    })

    # 4. Feature Engineering: Tạo cột văn bản tổng hợp
    cols_to_concat = [
        "prod_name", 
        "product_type_name", 
        "product_group_name", 
        "colour_group_name", 
        "department_name", 
        "detail_desc"
    ]
    
    print("4️⃣ Đang gộp cột và làm sạch Text...")
    # Gộp các cột lại, phân cách bằng khoảng trắng
    df_articles = df_articles.withColumn(
        "combined_text",
        F.concat_ws(" ", *[F.col(c) for c in cols_to_concat])
    )

    # Chuyển về chữ thường + Xóa ký tự lạ (chỉ giữ lại chữ cái và số)
    df_articles = df_articles.withColumn(
        "combined_text", 
        F.lower(F.col("combined_text"))
    )
    df_articles = df_articles.withColumn(
        "combined_text",
        F.regexp_replace(F.col("combined_text"), "[^a-z0-9\\s]", "")
    )

    # 5. NLP Pipeline (TF-IDF)
    print("5️⃣ Đang chạy NLP Pipeline (TF-IDF)...")
    
    tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    
    # numFeatures=2000: Giữ lại 2000 từ quan trọng nhất (giúp giảm nhẹ RAM)
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=2000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf])
    
    # Fit và Transform
    model = pipeline.fit(df_articles)
    df_articles_nlp = model.transform(df_articles)

    # ---------------------------------------------------------
    # BƯỚC QUAN TRỌNG: JOIN VỚI MAPPING ĐỂ LẤY item_id_int
    # ---------------------------------------------------------
    print("6️⃣ Đang JOIN với bảng Mapping để lấy ID chuẩn...")
    # Chỉ giữ lại những sản phẩm có trong file Mapping (Inner Join)
    df_final = df_articles_nlp.join(df_mapping, on="article_id", how="inner")

    # 7. Lọc cột và Lưu trữ
    print("7️⃣ Đang lưu kết quả...")
    # CHÚ Ý: Cột quan trọng nhất bây giờ là 'item_id_int' và 'features'
    df_output = df_final.select(
        "item_id_int",      # ID chuẩn dạng số (cho Model)
        "features",         # Vector đặc trưng (cho Model Content-based)
        "combined_text"     # (Optional) Giữ lại để kiểm tra nếu cần
    )

    df_output.show(5, truncate=True)

    # Lưu ra Parquet
    df_output.write.mode("overwrite").parquet(output_path)
    print(f"🎉 Hoàn tất! File chuẩn đã lưu tại: {output_path}")

if __name__ == "__main__":
    # Khởi tạo Spark Session (Cấu hình chuẩn 16GB)
    print("🚀 Đang khởi động Spark...")
    spark = SparkSession.builder \
        .appName("HM_Process_Articles_Fixed") \
        .master("local[*]") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
        
    # ĐƯỜNG DẪN FILE (Cần chỉnh lại cho đúng máy của M4 nếu cần)
    INPUT_FILE = "data/raw/articles.csv" 
    MAPPING_FILE = "data/processed/item_mapping.parquet" # File từ điển của M1
    OUTPUT_FILE = "data/processed/articles_processed.parquet"
    
    # Kiểm tra file đầu vào
    if os.path.exists(INPUT_FILE):
        process_articles_data(spark, INPUT_FILE, MAPPING_FILE, OUTPUT_FILE)
    else:
        print(f"❌ Không tìm thấy file gốc: {INPUT_FILE}")
    
    spark.stop()