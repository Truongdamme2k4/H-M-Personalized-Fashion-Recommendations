"""
Candidate generator: FP-Growth association rules.

Hai luồng:
  1. User candidates (đi qua union) — với mỗi customer, predict items họ sẽ mua
  2. Cart recommendations (không qua union) — với mỗi article, tìm items mua kèm
     → sinh cart_recommendations.json như notebook, export thẳng vào MongoDB
"""
from __future__ import annotations
import argparse
import json
import os
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (
    get_spark, get_time_windows, filter_window, write_parquet,
    evaluate_recall, PATHS, s3a_to_local,
    DEFAULT_HISTORY_DAYS,
)

HISTORY_DAYS = 42
TOP_N = 20
MIN_SUPPORT = 0.001       # demo subset ~7k baskets → ≥ 7 lần cùng xuất hiện
MIN_CONFIDENCE = 0.02
MAX_BASKET_SIZE = 20


def build_baskets(history_df):
    baskets = (
        history_df.select("customer_id", "t_dat", "article_id")
        .dropDuplicates()
        .groupBy("customer_id", "t_dat")
        .agg(F.collect_set("article_id").alias("items"))
    )
    return baskets.filter(
        (F.size("items") > 1) & (F.size("items") <= MAX_BASKET_SIZE)
    )


def generate_fpgrowth_candidates(history_df, top_n: int = TOP_N):
    baskets = build_baskets(history_df).cache()
    total = baskets.count()
    print(f"  baskets: {total:,} (minSupport={MIN_SUPPORT} → ≥ {int(total * MIN_SUPPORT)} lần)")
    if total == 0:
        return history_df.sparkSession.createDataFrame(
            [], "customer_id string, article_id string, fpgrowth_score double, strategy string"
        )

    fp = FPGrowth(itemsCol="items", minSupport=MIN_SUPPORT, minConfidence=MIN_CONFIDENCE)
    model = fp.fit(baskets)
    rules_count = model.associationRules.count()
    print(f"  rules: {rules_count:,}")
    if rules_count == 0:
        return history_df.sparkSession.createDataFrame(
            [], "customer_id string, article_id string, fpgrowth_score double, strategy string"
        )

    user_history = (
        history_df.select("customer_id", "article_id").dropDuplicates()
        .groupBy("customer_id").agg(F.collect_set("article_id").alias("items"))
    )

    # Loại các item đã có trong lịch sử khỏi prediction → chỉ giữ ứng viên mới
    predictions = (
        model.transform(user_history)
        .filter(F.size("prediction") > 0)
        .select(
            "customer_id",
            F.expr(f"slice(prediction, 1, {top_n})").alias("fp_items"),
        )
    )

    exploded = predictions.select(
        "customer_id",
        F.posexplode("fp_items").alias("rank", "article_id"),
    ).withColumn("fpgrowth_score_raw", (F.lit(top_n) - F.col("rank")).cast("double"))

    w = Window.partitionBy("customer_id")
    return (
        exploded
        .withColumn("max_score", F.max("fpgrowth_score_raw").over(w))
        .withColumn("min_score", F.min("fpgrowth_score_raw").over(w))
        .withColumn(
            "fpgrowth_score",
            F.when(F.col("max_score") == F.col("min_score"), 1.0)
             .otherwise((F.col("fpgrowth_score_raw") - F.col("min_score")) /
                        (F.col("max_score") - F.col("min_score")))
        )
        .select("customer_id", "article_id", "fpgrowth_score")
        .withColumn("strategy", F.lit("fpgrowth"))
    )


def main(run_date: str | None):
    spark = get_spark("CandidateFPGrowth", driver_mem="6g")
    transactions = spark.read.parquet(PATHS["transactions"])

    win = get_time_windows(transactions, HISTORY_DAYS, run_date)
    train_hist = filter_window(transactions, win["train_hist_start"], win["val_start"])
    test_hist = filter_window(transactions, win["test_hist_start"], win["test_start"])

    write_parquet(generate_fpgrowth_candidates(train_hist),
                  f"{PATHS['candidates']}/train_fpgrowth.parquet")
    test_cands = generate_fpgrowth_candidates(test_hist)
    write_parquet(test_cands, f"{PATHS['candidates']}/test_fpgrowth.parquet")

    evaluate_recall(test_cands, transactions, win["test_start"], win["max_date"], "FPGrowth test")
    spark.stop()


# ---------------------------------------------------------------------------
# Cart recommendations — item-to-item co-purchase (như notebook, không qua union)
# ---------------------------------------------------------------------------

def build_baskets_for_cart(history_df, max_basket_size=20):
    """Tạo basket (customer × ngày) nhưng giới hạn size để tránh noise."""
    return (
        history_df.select("customer_id", "t_dat", "article_id")
        .dropDuplicates()
        .groupBy("customer_id", "t_dat")
        .agg(F.collect_set("article_id").alias("items"))
    ).filter(
        (F.size("items") > 1) & (F.size("items") <= max_basket_size)
    )


def generate_cart_recommendations(history_df, top_n: int = 6,
                                  min_confidence: float = 0.02,
                                  min_support: float = 0.001):
    """
    Sinh cart recommendations: với mỗi article, tìm các article mua kèm.

    Khác với user candidates (user-to-item), ở đây là item-to-item:
    - Train FPGrowth trên baskets
    - Với mỗi antecedent (1 item), lấy consequent items từ rules
    - Output: {_id: article_id, items: [consequent_ids...]}
    """
    baskets = build_baskets_for_cart(history_df).cache()
    total = baskets.count()
    print(f"  Cart FPGrowth — baskets: {total:,}")

    fp = FPGrowth(itemsCol="items", minSupport=min_support,
                  minConfidence=min_confidence)
    model = fp.fit(baskets)

    # Lấy rules chỉ có 1 antecedent item (không cần multi-item antecedent cho cart)
    rules = (
        model.associationRules
        .filter(F.size(F.col("antecedent")) == 1)
        .withColumn("antecedent_item", F.expr("element_at(antecedent, 1)"))
        .filter(F.size(F.col("consequent")) > 0)
        .withColumn("consequent_items", F.col("consequent"))
    )

    # Với mỗi antecedent item, lấy top consequent items theo confidence
    w = Window.partitionBy("antecedent_item")
    ranked = (
        rules
        .withColumn("_rank", F.row_number().over(
            Window.partitionBy("antecedent_item")
                  .orderBy(F.desc("confidence"))
        ))
        .filter(F.col("_rank") <= top_n)
    )

    # Group by antecedent → list of consequents
    cart_recs = (
        ranked
        .groupBy("antecedent_item")
        .agg(F.collect_list(F.col("consequent_items")).alias("all_consequents"))
        .withColumn(
            "items",
            F.expr("slice(array_distinct(flatten(all_consequents)), 1, 6)")
        )
        .withColumnRenamed("antecedent_item", "article_id")
        .select("article_id", "items")
    )

    return cart_recs


def export_cart_recommendations_to_json(cart_df, output_path: str):
    """Export cart recommendations thành JSON {_id, items} như notebook."""
    records = cart_df.toPandas()
    # Dùng .iloc hoặc ['items'] thay vì .items vì .items là pandas method
    result = {row.article_id: row['items'] for _, row in records.iterrows()}
    with open(output_path, "w") as f:
        json.dump(result, f)
    print(f"  Cart recommendations: {len(result):,} articles → {output_path}")
    return result


def upload_cart_to_minio(local_path: str):
    """Upload cart_recommendations.json lên MinIO gold layer."""
    from common import local_to_s3a
    gold_path = f"{PATHS['predictions']}/cart_recommendations.json"
    local_to_s3a(local_path, gold_path)
    return gold_path


def main_cart(run_date: str | None, output_json: str):
    """
    Sinh cart_recommendations.json từ FPGrowth (item-to-item).
    Không qua union — xuất thẳng file JSON như notebook.
    Upload lên MinIO gold và trả về đường dẫn.
    """
    spark = get_spark("CartFPGrowth", driver_mem="2g")
    transactions = spark.read.parquet(PATHS["transactions"])

    win = get_time_windows(transactions, DEFAULT_HISTORY_DAYS, run_date)
    # Dùng toàn bộ history để có đủ signal cho association rules
    train_hist = filter_window(
        transactions,
        win["test_hist_start"],
        win["test_start"],
    )

    cart_df = generate_cart_recommendations(train_hist)
    result = export_cart_recommendations_to_json(cart_df, output_json)
    gold_path = upload_cart_to_minio(output_json)
    spark.stop()
    print("  Done.")
    return gold_path


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None)
    p.add_argument("--output_json", default="/tmp/cart_recommendations.json")
    p.add_argument("--cart_only", action="store_true",
                   help="Chỉ sinh cart_recommendations.json (không sinh user candidates)")
    args = p.parse_args()

    if args.cart_only:
        main_cart(args.run_date, args.output_json)
    else:
        main(p.parse_args().run_date)
