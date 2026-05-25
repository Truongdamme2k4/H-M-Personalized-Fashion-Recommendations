"""
Step 1: Data Cleaning
=====================
Đọc raw data từ HDFS → clean → lưu dạng parquet

Input:  /data/raw/transactions_train.csv
        /data/raw/articles.csv
        /data/raw/customers.csv

Output: /data/cleaned/transactions/
        /data/cleaned/articles/
        /data/cleaned/customers/
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, DateType
)

spark = (
    SparkSession.builder
    .appName("HM_Step1_Cleaning")
    .config("spark.sql.adaptive.enabled", "true")        # Tự tối ưu join/shuffle
    .config("spark.sql.shuffle.partitions", "8")          # Phù hợp cluster nhỏ (2 workers)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("=" * 60)
print("STEP 1: DATA CLEANING")
print("=" * 60)

# ============================================================
# 2. Đọc raw data từ HDFS
# ============================================================
HDFS_BASE = os.environ.get("HDFS_BASE", "hdfs://namenode:9000")

# --- Transactions (31M rows) ---
print("\n[1/3] Đọc transactions_train.csv...")
transactions_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")     # Tự define schema cho nhanh
    .csv(f"{HDFS_BASE}/data/raw/transactions_train.csv")
)

# Cast đúng type
transactions = (
    transactions_raw
    .withColumn("t_dat", F.to_date("t_dat", "yyyy-MM-dd"))
    .withColumn("customer_id", F.col("customer_id").cast(StringType()))
    .withColumn("article_id", F.col("article_id").cast(StringType()))
    .withColumn("price", F.col("price").cast(FloatType()))
    .withColumn("sales_channel_id", F.col("sales_channel_id").cast(IntegerType()))
)

print(f"  Raw transactions: {transactions.count():,} rows")

# --- Articles (105K rows) ---
print("\n[2/3] Đọc articles.csv...")
articles_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{HDFS_BASE}/data/raw/articles.csv")
)

# Cast article_id thành string (giữ leading zeros)
articles = articles_raw.withColumn(
    "article_id",
    F.lpad(F.col("article_id").cast(StringType()), 10, "0")
)

print(f"  Raw articles: {articles.count():,} rows")

# --- Customers (1.3M rows) ---
print("\n[3/3] Đọc customers.csv...")
customers_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{HDFS_BASE}/data/raw/customers.csv")
)

customers = customers_raw.withColumn(
    "customer_id", F.col("customer_id").cast(StringType())
)

print(f"  Raw customers: {customers.count():,} rows")

# ============================================================
# 3. Cleaning: Transactions
# ============================================================
print("\n--- Cleaning Transactions ---")

# Đếm null trước khi clean
null_counts = transactions.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in transactions.columns
])
print("  Null counts:")
null_counts.show(truncate=False)

# Clean
transactions_clean = (
    transactions
    # Bỏ rows thiếu thông tin quan trọng
    .dropna(subset=["customer_id", "article_id", "t_dat"])
    # Bỏ giá âm hoặc = 0 (data lỗi)
    .filter(F.col("price") > 0)
    # Bỏ duplicate (cùng customer, cùng article, cùng ngày, cùng giá)
    .dropDuplicates(["customer_id", "article_id", "t_dat", "price"])
    # Pad article_id về 10 ký tự (khớp với articles table)
    .withColumn(
        "article_id",
        F.lpad(F.col("article_id"), 10, "0")
    )
    # Thêm các cột thời gian hữu ích
    .withColumn("year", F.year("t_dat"))
    .withColumn("month", F.month("t_dat"))
    .withColumn("day_of_week", F.dayofweek("t_dat"))
    .withColumn("week_of_year", F.weekofyear("t_dat"))
)

print(f"  Sau cleaning: {transactions_clean.count():,} rows")

# ============================================================
# 4. Cleaning: Articles
# ============================================================
print("\n--- Cleaning Articles ---")

# Các cột quan trọng cho content-based model
ARTICLE_COLS = [
    "article_id",
    "product_code", "prod_name", "product_type_name",
    "product_group_name", "graphical_appearance_name",
    "colour_group_name", "perceived_colour_value_name",
    "perceived_colour_master_name",
    "department_name", "index_name", "index_group_name",
    "section_name", "garment_group_name",
    "detail_desc"
]

# Chỉ giữ cột cần thiết
articles_clean = (
    articles
    .select([c for c in ARTICLE_COLS if c in articles.columns])
    # Fill null cho description
    .fillna({"detail_desc": "no description"})
    # Fill null cho các categorical columns
    .fillna("Unknown", subset=[
        "graphical_appearance_name",
        "colour_group_name",
        "perceived_colour_value_name",
        "perceived_colour_master_name",
        "section_name",
        "garment_group_name"
    ])
)

print(f"  Articles cleaned: {articles_clean.count():,} rows")
print("  Sample:")
articles_clean.show(3, truncate=40)

# ============================================================
# 5. Cleaning: Customers
# ============================================================
print("\n--- Cleaning Customers ---")

customers_clean = (
    customers
    .select(
        "customer_id",
        "FN", "Active",
        "club_member_status",
        "fashion_news_frequency",
        "age", "postal_code"
    )
    # Fill null
    .fillna({
        "FN": 0.0,
        "Active": 0.0,
        "club_member_status": "UNKNOWN",
        "fashion_news_frequency": "NONE",
        "age": -1       # Flag unknown age
    })
    # Lọc age bất thường (< 10 hoặc > 100)
    .withColumn(
        "age",
        F.when(
            (F.col("age") >= 10) & (F.col("age") <= 100),
            F.col("age")
        ).otherwise(-1)
    )
    # Tạo age group
    .withColumn(
        "age_group",
        F.when(F.col("age") == -1, "Unknown")
        .when(F.col("age") < 20, "Teen")
        .when(F.col("age") < 30, "20s")
        .when(F.col("age") < 40, "30s")
        .when(F.col("age") < 50, "40s")
        .when(F.col("age") < 60, "50s")
        .otherwise("60+")
    )
)

print(f"  Customers cleaned: {customers_clean.count():,} rows")
print("  Age distribution:")
customers_clean.groupBy("age_group").count().orderBy("count", ascending=False).show()

# ============================================================
# 6. Validate: Kiểm tra join khớp
# ============================================================
print("\n--- Validation ---")

# Transactions phải join được với articles và customers
valid_articles = set(
    articles_clean.select("article_id").rdd.flatMap(lambda x: x).collect()
)
valid_customers = set(
    customers_clean.select("customer_id").rdd.flatMap(lambda x: x).collect()
)

# Broadcast join nhỏ (articles chỉ 105K)
transactions_validated = (
    transactions_clean
    .join(
        F.broadcast(articles_clean.select("article_id")),
        on="article_id",
        how="inner"
    )
    .join(
        customers_clean.select("customer_id"),
        on="customer_id",
        how="inner"
    )
)

final_count = transactions_validated.count()
print(f"  Transactions sau validate join: {final_count:,} rows")

# ============================================================
# 7. Lưu kết quả lên HDFS dạng Parquet
# ============================================================
print("\n--- Saving to HDFS (Parquet) ---")

OUTPUT_BASE = f"{HDFS_BASE}/data/cleaned"

# Transactions: partition theo year-month để query nhanh
print("  Saving transactions...")
transactions_validated.write.mode("overwrite").partitionBy("year", "month").parquet(
    f"{OUTPUT_BASE}/transactions"
)

print("  Saving articles...")
articles_clean.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/articles"
)

print("  Saving customers...")
customers_clean.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/customers"
)

print("\n" + "=" * 60)
print("STEP 1 DONE!")
print(f"  Transactions: {OUTPUT_BASE}/transactions/")
print(f"  Articles:     {OUTPUT_BASE}/articles/")
print(f"  Customers:    {OUTPUT_BASE}/customers/")
print("=" * 60)

spark.stop()
