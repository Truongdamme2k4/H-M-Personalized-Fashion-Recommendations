"""
Step 2: Feature Engineering
============================
Đọc cleaned data → tạo features cho cả user và item

Input:  /data/cleaned/transactions/
        /data/cleaned/articles/
        /data/cleaned/customers/

Output: /data/features/user_features/
        /data/features/item_features/
        /data/features/user_item_interactions/
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("HM_Step2_FeatureEngineering")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("=" * 60)
print("STEP 2: FEATURE ENGINEERING")
print("=" * 60)

HDFS = "hdfs://namenode:9000"

# ============================================================
# 1. Đọc cleaned data
# ============================================================
print("\nĐọc cleaned data...")
transactions = spark.read.parquet(f"{HDFS}/data/cleaned/transactions")
articles = spark.read.parquet(f"{HDFS}/data/cleaned/articles")
customers = spark.read.parquet(f"{HDFS}/data/cleaned/customers")

# Ngày cuối cùng trong dataset (dùng làm mốc tính recency)
max_date = transactions.agg(F.max("t_dat")).collect()[0][0]
print(f"  Ngày giao dịch cuối: {max_date}")
print(f"  Transactions: {transactions.count():,}")

# ============================================================
# 2. USER FEATURES (RFM + behavioral)
# ============================================================
print("\n--- Building User Features ---")

user_features = (
    transactions
    .groupBy("customer_id")
    .agg(
        # === RFM ===
        # Recency: Số ngày kể từ lần mua cuối
        F.datediff(F.lit(max_date), F.max("t_dat")).alias("recency_days"),

        # Frequency: Tổng số lần mua
        F.count("*").alias("total_purchases"),

        # Monetary: Tổng chi tiêu
        F.sum("price").alias("total_spent"),

        # === Behavioral ===
        # Số sản phẩm unique đã mua
        F.countDistinct("article_id").alias("unique_items_bought"),

        # Giá trung bình mỗi lần mua
        F.avg("price").alias("avg_price"),

        # Giá cao nhất / thấp nhất
        F.max("price").alias("max_price"),
        F.min("price").alias("min_price"),

        # Độ lệch chuẩn giá (đa dạng giá cả)
        F.stddev("price").alias("price_stddev"),

        # Số ngày unique có giao dịch
        F.countDistinct("t_dat").alias("active_days"),

        # Kênh mua hàng chủ yếu (1=online, 2=offline)
        F.avg("sales_channel_id").alias("avg_channel"),

        # Ngày mua đầu tiên
        F.min("t_dat").alias("first_purchase_date"),

        # === Temporal ===
        # Mua nhiều nhất vào ngày nào trong tuần (mode)
        F.mode("day_of_week").alias("favorite_day_of_week"),
    )
    # Tính thêm derived features
    .withColumn(
        # Customer lifetime (ngày): first purchase → last purchase
        "customer_lifetime_days",
        F.datediff(F.lit(max_date), F.col("first_purchase_date"))
    )
    .withColumn(
        # Tần suất mua: purchases / lifetime
        "purchase_frequency",
        F.when(
            F.col("customer_lifetime_days") > 0,
            F.col("total_purchases") / F.col("customer_lifetime_days")
        ).otherwise(0.0)
    )
    .withColumn(
        # Diversity score: unique items / total purchases
        "diversity_score",
        F.col("unique_items_bought") / F.col("total_purchases")
    )
    .drop("first_purchase_date")    # Không cần giữ raw date
    .fillna(0.0, subset=["price_stddev"])   # stddev null khi chỉ có 1 purchase
)

# Join thêm thông tin customer demographics
user_features = user_features.join(
    customers.select("customer_id", "age", "age_group", "club_member_status"),
    on="customer_id",
    how="left"
)

print(f"  User features: {user_features.count():,} users")
print("  Schema:")
user_features.printSchema()
print("  Sample:")
user_features.show(3, truncate=False)

# ============================================================
# 3. ITEM FEATURES (article metadata + popularity)
# ============================================================
print("\n--- Building Item Features ---")

# Tính popularity metrics từ transactions
item_popularity = (
    transactions
    .groupBy("article_id")
    .agg(
        # Tổng lượt mua
        F.count("*").alias("total_sales"),

        # Số khách unique đã mua
        F.countDistinct("customer_id").alias("unique_buyers"),

        # Doanh thu
        F.sum("price").alias("total_revenue"),

        # Giá trung bình thực tế
        F.avg("price").alias("avg_actual_price"),

        # Lần cuối được mua (hot hay cold item)
        F.datediff(F.lit(max_date), F.max("t_dat")).alias("days_since_last_sold"),

        # Tỷ lệ mua online vs offline
        F.avg("sales_channel_id").alias("avg_sales_channel"),

        # Số tháng khác nhau có bán (seasonality indicator)
        F.countDistinct("month").alias("months_active"),
    )
)

# Tính popularity rank
window_pop = Window.orderBy(F.desc("total_sales"))
item_popularity = item_popularity.withColumn(
    "popularity_rank",
    F.dense_rank().over(window_pop)
)

# Tạo text features cho content-based model
# Gộp các categorical columns thành 1 string (dùng cho TF-IDF sau)
articles_with_text = articles.withColumn(
    "content_text",
    F.concat_ws(
        " ",
        F.col("product_type_name"),
        F.col("product_group_name"),
        F.col("graphical_appearance_name"),
        F.col("colour_group_name"),
        F.col("perceived_colour_value_name"),
        F.col("perceived_colour_master_name"),
        F.col("department_name"),
        F.col("section_name"),
        F.col("garment_group_name"),
        F.col("detail_desc")
    )
)

# Join metadata + popularity
item_features = (
    articles_with_text
    .join(item_popularity, on="article_id", how="left")
    .fillna(0, subset=["total_sales", "unique_buyers", "total_revenue"])
    .fillna(9999, subset=["days_since_last_sold"])
)

print(f"  Item features: {item_features.count():,} items")
print(f"  Items có sales data: {item_popularity.count():,}")
print("  Top 5 best sellers:")
(
    item_features
    .select("article_id", "prod_name", "total_sales", "unique_buyers")
    .orderBy("total_sales", ascending=False)
    .show(5, truncate=40)
)

# ============================================================
# 4. USER-ITEM INTERACTION MATRIX
# ============================================================
print("\n--- Building User-Item Interaction Matrix ---")

# Tạo interaction score (không chỉ binary 0/1)
# Score = số lần mua * recency weight
user_item = (
    transactions
    .groupBy("customer_id", "article_id")
    .agg(
        # Số lần user mua item này
        F.count("*").alias("purchase_count"),

        # Lần gần nhất mua
        F.max("t_dat").alias("last_purchase"),

        # Tổng chi cho item này
        F.sum("price").alias("total_spent_on_item"),
    )
    # Tính recency weight: mua gần đây → weight cao hơn
    .withColumn(
        "days_ago",
        F.datediff(F.lit(max_date), F.col("last_purchase"))
    )
    .withColumn(
        # Exponential decay: e^(-days/180)
        # Mua trong 6 tháng gần = weight cao, mua từ lâu = weight thấp
        "recency_weight",
        F.exp(-F.col("days_ago") / 180.0)
    )
    .withColumn(
        # Final interaction score
        # = log(1 + purchase_count) * recency_weight
        # Log để giảm ảnh hưởng của outlier (user mua 50 lần cùng 1 item)
        "interaction_score",
        F.log1p(F.col("purchase_count")) * F.col("recency_weight")
    )
    .select(
        "customer_id", "article_id",
        "purchase_count", "interaction_score",
        "days_ago", "recency_weight"
    )
)

print(f"  Unique interactions: {user_item.count():,}")
print(f"  Sparsity check:")

n_users = user_item.select("customer_id").distinct().count()
n_items = user_item.select("article_id").distinct().count()
n_interactions = user_item.count()
sparsity = 1 - (n_interactions / (n_users * n_items))

print(f"    Users:  {n_users:,}")
print(f"    Items:  {n_items:,}")
print(f"    Interactions: {n_interactions:,}")
print(f"    Sparsity: {sparsity:.6f} ({sparsity*100:.4f}%)")
print("    (> 99% là bình thường cho recommendation)")

print("\n  Interaction score distribution:")
user_item.select("interaction_score").describe().show()

print("  Sample interactions:")
user_item.orderBy("interaction_score", ascending=False).show(5, truncate=False)

# ============================================================
# 5. INDEX MAPPING (cho ALS model)
# ============================================================
print("\n--- Creating Index Mappings ---")

# ALS cần integer IDs, không dùng string
from pyspark.ml.feature import StringIndexer

# Map customer_id → integer index
customer_indexer = StringIndexer(
    inputCol="customer_id",
    outputCol="customer_idx"
).fit(user_item)

item_indexer = StringIndexer(
    inputCol="article_id",
    outputCol="article_idx"
).fit(user_item)

# Apply mapping
user_item_indexed = (
    customer_indexer.transform(user_item)
    .transform(lambda df: item_indexer.transform(df))
)

# Cast to int (ALS cần IntegerType)
user_item_indexed = (
    user_item_indexed
    .withColumn("customer_idx", F.col("customer_idx").cast(IntegerType()))
    .withColumn("article_idx", F.col("article_idx").cast(IntegerType()))
)

from pyspark.sql.types import IntegerType

user_item_indexed = (
    user_item_indexed
    .withColumn("customer_idx", F.col("customer_idx").cast(IntegerType()))
    .withColumn("article_idx", F.col("article_idx").cast(IntegerType()))
)

print(f"  Customer index range: 0 → {n_users - 1}")
print(f"  Article index range:  0 → {n_items - 1}")

# Lưu mapping tables (để decode kết quả sau)
customer_mapping = customer_indexer.transform(
    user_item.select("customer_id").distinct()
).select("customer_id", F.col("customer_idx").cast(IntegerType()))

item_mapping = item_indexer.transform(
    user_item.select("article_id").distinct()
).select("article_id", F.col("article_idx").cast(IntegerType()))

# ============================================================
# 6. TRAIN/TEST SPLIT (time-based)
# ============================================================
print("\n--- Time-based Train/Test Split ---")

# Split theo thời gian: 3 tuần cuối = test, còn lại = train
# Đây là cách split đúng cho recommendation (không random split)
from datetime import timedelta

split_date = max_date - timedelta(days=21)
print(f"  Split date: {split_date}")
print(f"  Train: before {split_date}")
print(f"  Test:  {split_date} → {max_date}")

train_interactions = user_item_indexed.filter(
    F.col("days_ago") > 21    # Mua trước split date
)
test_interactions = user_item_indexed.filter(
    F.col("days_ago") <= 21   # Mua trong 3 tuần cuối
)

train_count = train_interactions.count()
test_count = test_interactions.count()
print(f"  Train: {train_count:,} interactions")
print(f"  Test:  {test_count:,} interactions")
print(f"  Ratio: {train_count/(train_count+test_count)*100:.1f}% / {test_count/(train_count+test_count)*100:.1f}%")

# ============================================================
# 7. LƯU TẤT CẢ LÊN HDFS
# ============================================================
print("\n--- Saving to HDFS ---")

OUTPUT = f"{HDFS}/data/features"

print("  Saving user_features...")
user_features.write.mode("overwrite").parquet(f"{OUTPUT}/user_features")

print("  Saving item_features...")
item_features.write.mode("overwrite").parquet(f"{OUTPUT}/item_features")

print("  Saving user_item_interactions (full)...")
user_item_indexed.write.mode("overwrite").parquet(f"{OUTPUT}/user_item_interactions")

print("  Saving train set...")
train_interactions.write.mode("overwrite").parquet(f"{OUTPUT}/train")

print("  Saving test set...")
test_interactions.write.mode("overwrite").parquet(f"{OUTPUT}/test")

print("  Saving index mappings...")
customer_mapping.write.mode("overwrite").parquet(f"{OUTPUT}/customer_mapping")
item_mapping.write.mode("overwrite").parquet(f"{OUTPUT}/item_mapping")

# ============================================================
# 8. SUMMARY
# ============================================================
print("\n" + "=" * 60)
print("STEP 2 DONE! Summary:")
print("=" * 60)
print(f"""
  USER FEATURES ({n_users:,} users):
    - RFM: recency_days, total_purchases, total_spent
    - Behavioral: unique_items, avg_price, diversity_score
    - Temporal: purchase_frequency, favorite_day_of_week
    - Demographics: age, age_group, club_member_status

  ITEM FEATURES ({item_features.count():,} items):
    - Metadata: product_type, colour, section, garment_group...
    - Content text: gộp metadata → string (cho TF-IDF)
    - Popularity: total_sales, unique_buyers, popularity_rank
    - Recency: days_since_last_sold

  USER-ITEM MATRIX ({n_interactions:,} interactions):
    - interaction_score = log(1 + count) × recency_weight
    - Indexed: customer_idx (int), article_idx (int)
    - Split: train ({train_count:,}) / test ({test_count:,})

  Output: {OUTPUT}/
""")

spark.stop()
