"""
Candidate generator: Repurchase — lấy Top-N item mới mua gần nhất của mỗi user.
"""
from __future__ import annotations
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (
    get_spark, get_time_windows, filter_window, write_parquet,
    evaluate_recall, PATHS,
)

HISTORY_DAYS = 42
TOP_N = 15


def generate_repurchase_candidates(history_df, top_n: int = TOP_N):
    user_item_latest = (
        history_df.groupBy("customer_id", "article_id")
        .agg(F.max("t_dat").alias("latest_buy_date"))
    )
    w = Window.partitionBy("customer_id").orderBy(F.col("latest_buy_date").desc())
    return (
        user_item_latest.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") <= top_n)
        .select("customer_id", "article_id")
        .withColumn("strategy", F.lit("repurchase"))
    )


def main(run_date: str | None):
    spark = get_spark("CandidateRepurchase", driver_mem="8g")
    transactions = spark.read.parquet(PATHS["transactions"])

    win = get_time_windows(transactions, HISTORY_DAYS, run_date)
    train_hist = filter_window(transactions, win["train_hist_start"], win["val_start"])
    test_hist = filter_window(transactions, win["test_hist_start"], win["test_start"])

    write_parquet(generate_repurchase_candidates(train_hist),
                  f"{PATHS['candidates']}/train_repurchase.parquet")
    test_cands = generate_repurchase_candidates(test_hist)
    write_parquet(test_cands, f"{PATHS['candidates']}/test_repurchase.parquet")

    evaluate_recall(test_cands, transactions, win["test_start"], win["max_date"], "Repurchase test")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None)
    main(p.parse_args().run_date)
