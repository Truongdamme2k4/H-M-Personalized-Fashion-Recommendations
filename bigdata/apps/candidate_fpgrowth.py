"""
Candidate generator: FP-Growth association rules.
Tạo basket (customer × ngày) → train FPGrowth → transform user history → Top-20 ứng viên.
"""
from __future__ import annotations
import argparse
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (
    get_spark, get_time_windows, filter_window, write_parquet,
    evaluate_recall, PATHS,
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


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None)
    main(p.parse_args().run_date)
