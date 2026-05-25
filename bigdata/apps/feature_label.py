"""
Gắn nhãn cho train master (label=1 nếu user thực sự mua item trong tuần target).
Downsample 10:1 negatives, sinh 22 đặc trưng (RFM + xu hướng + categorical).
"""
from __future__ import annotations
import argparse
import datetime
from pyspark.sql import functions as F

from common import get_spark, get_time_windows, filter_window, write_parquet, PATHS


HISTORY_DAYS = 42


def calculate_features(base_df, transactions, customers, articles, end_date):
    start_date = end_date - datetime.timedelta(days=HISTORY_DAYS)
    hist = filter_window(transactions, start_date, end_date)

    customers_meta = customers.select("customer_id", "age").withColumn(
        "age_group",
        F.when(F.col("age") < 25, "<25")
         .when((F.col("age") >= 25) & (F.col("age") <= 35), "25-35")
         .when((F.col("age") >= 36) & (F.col("age") <= 45), "36-45")
         .when((F.col("age") >= 46) & (F.col("age") <= 55), "46-55")
         .otherwise(">55")
    )

    item_features = hist.groupBy("article_id").agg(
        F.count("customer_id").alias("item_total_sales"),
        F.avg("price").alias("item_avg_price"),
    )
    user_features = hist.groupBy("customer_id").agg(
        F.count("article_id").alias("user_total_purchases"),
        F.avg("price").alias("user_avg_budget"),
    )
    user_item_interaction = hist.groupBy("customer_id", "article_id") \
        .agg(F.count("t_dat").alias("user_item_buy_count"))

    user_recency = hist.groupBy("customer_id").agg(F.max("t_dat").alias("last_purchase_date")) \
        .withColumn("days_since_last_purchase",
                    F.datediff(F.lit(end_date), F.col("last_purchase_date"))) \
        .select("customer_id", "days_since_last_purchase")

    item_recency = hist.groupBy("customer_id", "article_id") \
        .agg(F.max("t_dat").alias("last_bought_this_item")) \
        .withColumn("days_since_bought_THIS_item",
                    F.datediff(F.lit(end_date), F.col("last_bought_this_item"))) \
        .select("customer_id", "article_id", "days_since_bought_THIS_item")

    def trend(days, col):
        return hist.filter(F.col("t_dat") >= end_date - datetime.timedelta(days=days)) \
            .groupBy("article_id").agg(F.count("customer_id").alias(col))

    t3, t7, t14 = trend(3, "item_sales_last_3d"), trend(7, "item_sales_last_7d"), trend(14, "item_sales_last_14d")

    hist_with_type = hist.join(F.broadcast(articles.select("article_id", "product_type_name")),
                                "article_id", "inner")
    user_type = hist_with_type.groupBy("customer_id", "product_type_name") \
        .agg(F.count("*").alias("user_type_buy_count"))

    hist_age = hist.join(F.broadcast(customers.select("customer_id", "age")), "customer_id", "inner")
    item_avg_age = hist_age.groupBy("article_id").agg(F.avg("age").alias("item_avg_age"))

    hist_ag = hist.join(F.broadcast(customers_meta.select("customer_id", "age_group")),
                         "customer_id", "inner")
    age_group_item = hist_ag.groupBy("age_group", "article_id") \
        .agg(F.count("*").alias("age_group_item_sales"))

    arts_meta = articles.select("article_id", "product_type_name", "colour_group_name")

    df = (
        base_df.join(F.broadcast(customers_meta), "customer_id", "left")
               .join(F.broadcast(arts_meta), "article_id", "left")
               .join(F.broadcast(item_features), "article_id", "left")
               .join(F.broadcast(user_features), "customer_id", "left")
               .join(F.broadcast(user_recency), "customer_id", "left")
               .join(user_item_interaction, ["customer_id", "article_id"], "left")
               .join(item_recency, ["customer_id", "article_id"], "left")
               .join(F.broadcast(t3), "article_id", "left")
               .join(F.broadcast(t7), "article_id", "left")
               .join(F.broadcast(t14), "article_id", "left")
               .join(F.broadcast(item_avg_age), "article_id", "left")
               .join(F.broadcast(user_type), ["customer_id", "product_type_name"], "left")
               .join(F.broadcast(age_group_item), ["age_group", "article_id"], "left")
    )

    df = df.fillna({
        "item_total_sales": 0, "item_avg_price": 0.02,
        "user_total_purchases": 0, "user_avg_budget": 0.02,
        "user_item_buy_count": 0, "days_since_last_purchase": 999,
        "days_since_bought_THIS_item": 999,
        "item_sales_last_3d": 0, "item_sales_last_7d": 0, "item_sales_last_14d": 0,
        "age_group_item_sales": 0, "age": 25,
        "product_type_name": "Unknown", "colour_group_name": "Unknown",
        "user_type_buy_count": 0, "item_avg_age": 25,
        "als_score": 0.0, "itemcf_score": 0.0,
    })

    df = df.withColumn("price_diff", F.abs(F.col("item_avg_price") - F.col("user_avg_budget"))) \
           .withColumn("age_diff", F.abs(F.col("age") - F.col("item_avg_age"))) \
           .withColumn("trend_velocity",
                       F.col("item_sales_last_7d") / (F.col("item_sales_last_14d") + 1.0)) \
           .withColumn("from_als", F.when(F.array_contains(F.col("sources"), "als"), 1).otherwise(0)) \
           .withColumn("from_itemcf", F.when(F.array_contains(F.col("sources"), "itemcf"), 1).otherwise(0))

    return df.drop("sources", "age_group")


def main(run_date: str | None):
    spark = get_spark("FeatureLabel", driver_mem="10g")

    train_master = spark.read.parquet(f"{PATHS['master']}/train_master_candidates.parquet")
    test_master = spark.read.parquet(f"{PATHS['master']}/test_master_candidates.parquet")
    transactions = spark.read.parquet(PATHS["transactions"])
    customers = spark.read.parquet(PATHS["customers"])
    articles = spark.read.parquet(PATHS["articles"])

    win = get_time_windows(transactions, run_date=run_date)

    target = filter_window(transactions, win["val_start"], win["test_start"]) \
        .select("customer_id", "article_id").dropDuplicates() \
        .withColumn("label", F.lit(1))

    train_labeled = train_master.join(F.broadcast(target), ["customer_id", "article_id"], "left") \
        .fillna({"label": 0})

    pos = train_labeled.filter(F.col("label") == 1)
    neg = train_labeled.filter(F.col("label") == 0)
    pos_count = pos.count()
    neg_count = neg.count()
    fraction = min(1.0, (pos_count * 10) / neg_count if neg_count > 0 else 1.0)
    neg_sampled = neg.sample(withReplacement=False, fraction=fraction, seed=42)
    train_base = pos.unionByName(neg_sampled)
    print(f"Positives: {pos_count:,} | Negatives sampled: {int(neg_count * fraction):,}")

    print("Computing TRAIN features...")
    train_enriched = calculate_features(train_base, transactions, customers, articles, win["val_start"])
    write_parquet(train_enriched, f"{PATHS['master']}/train_enriched.parquet")

    print("Computing TEST features...")
    test_enriched = calculate_features(test_master, transactions, customers, articles, win["test_start"])
    write_parquet(test_enriched, f"{PATHS['master']}/test_enriched.parquet")

    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None)
    main(p.parse_args().run_date)
