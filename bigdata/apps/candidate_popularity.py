"""
Candidate generator: Popularity (Top-N best sellers 7 ngày gần nhất).
Broadcast cho mọi user — fallback cold-start.
"""
from __future__ import annotations
import argparse
import datetime
from pyspark.sql import functions as F

from common import (
    get_spark, get_time_windows, filter_window, write_parquet,
    evaluate_recall, PATHS,
)

HISTORY_DAYS = 42
TOP_N = 30
RECENT_DAYS = 7


def generate_popularity_candidates(history_df, top_n: int = TOP_N):
    hist_max = history_df.select(F.max("t_dat")).collect()[0][0]
    recent_start = hist_max - datetime.timedelta(days=RECENT_DAYS)

    top_items = (
        history_df.filter(F.col("t_dat") >= recent_start)
        .groupBy("article_id").count()
        .orderBy(F.col("count").desc())
        .limit(top_n).select("article_id")
    )
    unique_users = history_df.select("customer_id").dropDuplicates()
    return unique_users.crossJoin(F.broadcast(top_items)) \
        .withColumn("strategy", F.lit("popularity"))


def main(run_date: str | None):
    spark = get_spark("CandidatePopularity", driver_mem="8g")
    transactions = spark.read.parquet(PATHS["transactions"])

    win = get_time_windows(transactions, HISTORY_DAYS, run_date)
    train_hist = filter_window(transactions, win["train_hist_start"], win["val_start"])
    test_hist = filter_window(transactions, win["test_hist_start"], win["test_start"])

    write_parquet(generate_popularity_candidates(train_hist),
                  f"{PATHS['candidates']}/train_popularity.parquet")
    test_cands = generate_popularity_candidates(test_hist)
    write_parquet(test_cands, f"{PATHS['candidates']}/test_popularity.parquet")

    evaluate_recall(test_cands, transactions, win["test_start"], win["max_date"], "Popularity test")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None)
    main(p.parse_args().run_date)
