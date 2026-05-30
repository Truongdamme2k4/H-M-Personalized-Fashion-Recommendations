"""
Step 1: Data Cleaning
=====================
Đọc bronze parquet từ MinIO → clean → lưu silver parquet.

Input  (bronze):  s3a://datalake/bronze/{transactions,articles,customers}
Output (silver):  s3a://datalake/silver/{transactions,articles,customers}
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType

from common import get_spark, PATHS


spark = get_spark("HM_Step1_Cleaning")
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("STEP 1: DATA CLEANING (bronze → silver)")
print("=" * 60)

# ============================================================
# Đọc bronze
# ============================================================
print("\n[1/3] Đọc bronze/transactions...")
transactions_raw = spark.read.parquet(PATHS["bronze_transactions"])
transactions = (
    transactions_raw
    .withColumn("t_dat", F.col("t_dat").cast("date"))
    .withColumn("customer_id", F.col("customer_id").cast(StringType()))
    .withColumn("article_id", F.col("article_id").cast(StringType()))
    .withColumn("price", F.col("price").cast(FloatType()))
    .withColumn("sales_channel_id", F.col("sales_channel_id").cast(IntegerType()))
)
print(f"  Raw transactions: {transactions.count():,} rows")

print("\n[2/3] Đọc bronze/articles...")
articles = spark.read.parquet(PATHS["bronze_articles"]).withColumn(
    "article_id", F.lpad(F.col("article_id").cast(StringType()), 10, "0")
)
print(f"  Raw articles: {articles.count():,} rows")

print("\n[3/3] Đọc bronze/customers...")
customers = spark.read.parquet(PATHS["bronze_customers"]).withColumn(
    "customer_id", F.col("customer_id").cast(StringType())
)
# OLTP column thường lowercase (fn, active) — map về tên CSV gốc (FN, Active)
for src, dst in [("fn", "FN"), ("active", "Active")]:
    if src in customers.columns:
        customers = customers.withColumnRenamed(src, dst)
# Postgres NUMERIC → Spark Decimal → pandas object (LightGBM reject)
# Cast tường minh sang Double để downstream xài được
for col in ["FN", "Active", "age"]:
    if col in customers.columns:
        customers = customers.withColumn(col, F.col(col).cast("double"))
print(f"  Raw customers: {customers.count():,} rows")

# ============================================================
# Cleaning: Transactions
# ============================================================
print("\n--- Cleaning Transactions ---")

null_counts = transactions.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in transactions.columns
])
print("  Null counts:")
null_counts.show(truncate=False)

transactions_clean = (
    transactions
    .dropna(subset=["customer_id", "article_id", "t_dat"])
    .filter(F.col("price") > 0)
    .dropDuplicates(["customer_id", "article_id", "t_dat", "price"])
    .withColumn("article_id", F.lpad(F.col("article_id"), 10, "0"))
    .withColumn("year", F.year("t_dat"))
    .withColumn("month", F.month("t_dat"))
    .withColumn("day_of_week", F.dayofweek("t_dat"))
    .withColumn("week_of_year", F.weekofyear("t_dat"))
)
print(f"  Sau cleaning: {transactions_clean.count():,} rows")

# ============================================================
# Cleaning: Articles
# ============================================================
print("\n--- Cleaning Articles ---")

ARTICLE_COLS = [
    "article_id",
    "product_code", "prod_name", "product_type_name",
    "product_group_name", "graphical_appearance_name",
    "colour_group_name", "perceived_colour_value_name",
    "perceived_colour_master_name",
    "department_name", "index_name", "index_group_name",
    "section_name", "garment_group_name",
    "detail_desc",
]

articles_clean = (
    articles
    .select([c for c in ARTICLE_COLS if c in articles.columns])
    .fillna({"detail_desc": "no description"})
    .fillna("Unknown", subset=[
        "graphical_appearance_name",
        "colour_group_name",
        "perceived_colour_value_name",
        "perceived_colour_master_name",
        "section_name",
        "garment_group_name",
    ])
)
print(f"  Articles cleaned: {articles_clean.count():,} rows")

# ============================================================
# Cleaning: Customers
# ============================================================
print("\n--- Cleaning Customers ---")

customers_clean = (
    customers
    .select("customer_id", "FN", "Active", "club_member_status",
            "fashion_news_frequency", "age", "postal_code")
    .fillna({
        "FN": 0.0,
        "Active": 0.0,
        "club_member_status": "UNKNOWN",
        "fashion_news_frequency": "NONE",
        "age": -1,
    })
    .withColumn(
        "age",
        F.when((F.col("age") >= 10) & (F.col("age") <= 100), F.col("age")).otherwise(-1),
    )
    .withColumn(
        "age_group",
        F.when(F.col("age") == -1, "Unknown")
         .when(F.col("age") < 20, "Teen")
         .when(F.col("age") < 30, "20s")
         .when(F.col("age") < 40, "30s")
         .when(F.col("age") < 50, "40s")
         .when(F.col("age") < 60, "50s")
         .otherwise("60+"),
    )
)
print(f"  Customers cleaned: {customers_clean.count():,} rows")

# ============================================================
# Validate join
# ============================================================
print("\n--- Validation ---")
transactions_validated = (
    transactions_clean
    .join(F.broadcast(articles_clean.select("article_id")), "article_id", "inner")
    .join(customers_clean.select("customer_id"), "customer_id", "inner")
)
print(f"  Transactions sau validate join: {transactions_validated.count():,} rows")

# ============================================================
# Save silver
# ============================================================
print("\n--- Saving silver (MinIO) ---")

print(f"  → {PATHS['transactions']}")
transactions_validated.write.mode("overwrite").partitionBy("year", "month").parquet(PATHS["transactions"])

print(f"  → {PATHS['articles']}")
articles_clean.write.mode("overwrite").parquet(PATHS["articles"])

print(f"  → {PATHS['customers']}")
customers_clean.write.mode("overwrite").parquet(PATHS["customers"])

print("\n" + "=" * 60)
print("STEP 1 DONE.")
print("=" * 60)

spark.stop()
