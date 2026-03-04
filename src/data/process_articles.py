from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml import Pipeline

def process_articles_data(spark, input_path, output_path):
    print("Đang đọc file articles.csv...")
    df_articles = spark.read.csv(input_path, header=True, inferSchema=True)

    # 1. Xử lý Missing Values
    print("Đang xử lý missing values...")
    df_articles = df_articles.fillna({
        "detail_desc": "unknown", 
        "prod_name": "unknown",
        "department_name": "unknown"
    })

    # 2. Feature Engineering: Tạo cột văn bản tổng hợp
    # Nối các cột chứa thông tin quan trọng thành một đoạn văn bản duy nhất
    # để phục vụ cho Content-Based Filtering sau này
    cols_to_concat = [
        "prod_name", 
        "product_type_name", 
        "product_group_name", 
        "colour_group_name", 
        "department_name", 
        "detail_desc"
    ]
    
    print("Đang tổng hợp text metadata...")
    df_articles = df_articles.withColumn(
        "combined_text",
        F.concat_ws(" ", *[F.col(c) for c in cols_to_concat])
    )

    # Chuyển toàn bộ text về chữ thường
    df_articles = df_articles.withColumn("combined_text", F.lower(F.col("combined_text")))

    # 3. NLP Pipeline bằng PySpark MLlib
    print("Đang chạy NLP Pipeline (TF-IDF)...")
    
    # Bước 3.1: Tách từ
    tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
    
    # Bước 3.2: Xóa stopwords
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    
    # Bước 3.3: Chuyển đổi text thành vector tần suất
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=2000)
    
    # Bước 3.4: Tính IDF 
    idf = IDF(inputCol="rawFeatures", outputCol="features")

    # Đóng gói vào Pipeline và chạy
    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf])
    model = pipeline.fit(df_articles)
    df_articles_nlp = model.transform(df_articles)

    # 4. Lọc lại các cột cần thiết và lưu trữ
    print("Đang lưu dữ liệu đã xử lý...")
    df_final = df_articles_nlp.select(
        "article_id", 
        "combined_text", 
        "features" # Cột này là vector TF-IDF
    )

    # In 5 dòng đầu 
    df_final.show(5, truncate=False)

    # Lưu ra định dạng Parquet
    df_final.write.mode("overwrite").parquet(output_path)
    print(f"Hoàn tất! Dữ liệu đã được lưu tại: {output_path}")

if __name__ == "__main__":
    # Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("HM_Process_Articles") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
        
    INPUT_FILE = "data/raw/articles.csv" 
    OUTPUT_FILE = "data/processed/articles_processed.parquet"
    
    process_articles_data(spark, INPUT_FILE, OUTPUT_FILE)
    
    spark.stop()