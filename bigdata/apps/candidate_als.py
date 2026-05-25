"""
Candidate generator: ALS (Spark ML, implicit feedback).
Sinh Top-40 ứng viên + als_score chuẩn hoá Min-Max per user.
"""
from __future__ import annotations
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer

from common import (
    get_spark, get_time_windows, filter_window, write_parquet,
    evaluate_recall, PATHS,
)

HISTORY_DAYS = 90
TOP_N = 40


def generate_als_candidates(history_df, top_n: int = TOP_N):
    user_idx = StringIndexer(inputCol="customer_id", outputCol="user_idx", handleInvalid="keep").fit(history_df)
    item_idx = StringIndexer(inputCol="article_id", outputCol="item_idx", handleInvalid="keep").fit(history_df)

    df = item_idx.transform(user_idx.transform(history_df))
    ratings = df.groupBy("user_idx", "item_idx").count().withColumnRenamed("count", "rating")

    als = ALS(
        maxIter=15, rank=32, regParam=0.05, alpha=40.0,
        userCol="user_idx", itemCol="item_idx", ratingCol="rating",
        coldStartStrategy="drop", implicitPrefs=True, seed=42,
    )
    model = als.fit(ratings)

    recs = model.recommendForAllUsers(top_n) \
        .select("user_idx", F.explode("recommendations").alias("rec")) \
        .select(
            "user_idx",
            F.col("rec.item_idx").alias("item_idx"),
            F.col("rec.rating").alias("als_score_raw"),
        )

    w = Window.partitionBy("user_idx")
    recs_scaled = (
        recs.withColumn("max_score", F.max("als_score_raw").over(w))
            .withColumn("min_score", F.min("als_score_raw").over(w))
            .withColumn(
                "als_score",
                F.when(F.col("max_score") == F.col("min_score"), 1.0)
                 .otherwise((F.col("als_score_raw") - F.col("min_score")) /
                            (F.col("max_score") - F.col("min_score")))
            )
    )

    user_map = df.select("customer_id", "user_idx").dropDuplicates()
    item_map = df.select("article_id", "item_idx").dropDuplicates()

    return (
        recs_scaled
        .join(user_map, "user_idx", "inner")
        .join(item_map, "item_idx", "inner")
        .select("customer_id", "article_id", "als_score")
        .withColumn("strategy", F.lit("als"))
    )


def main(run_date: str | None):
    spark = get_spark("CandidateALS", driver_mem="10g")
    transactions = spark.read.parquet(PATHS["transactions"])

    win = get_time_windows(transactions, HISTORY_DAYS, run_date)
    print(f"Train hist: {win['train_hist_start']} -> {win['val_start']}")
    print(f"Test hist:  {win['test_hist_start']} -> {win['test_start']}")

    train_hist = filter_window(transactions, win["train_hist_start"], win["val_start"])
    test_hist = filter_window(transactions, win["test_hist_start"], win["test_start"])

    train_cands = generate_als_candidates(train_hist, TOP_N)
    write_parquet(train_cands, f"{PATHS['candidates']}/train_als.parquet")

    test_cands = generate_als_candidates(test_hist, TOP_N)
    write_parquet(test_cands, f"{PATHS['candidates']}/test_als.parquet")

    evaluate_recall(test_cands, transactions, win["test_start"], win["max_date"], "ALS test")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None, help="YYYY-MM-DD (mặc định: max ngày trong dataset)")
    main(p.parse_args().run_date)
