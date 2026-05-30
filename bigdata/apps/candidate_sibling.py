"""
Candidate generator: Sibling product — các biến thể (màu/size) cùng product_code.
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


def generate_sibling_candidates(history_df, articles_df, top_n: int = TOP_N):
    # H&M product_code = 7 chữ số đầu của article_id (10 chữ số)
    # Luôn derive cùng cách trên cả 2 phía để tránh schema drift
    history_pc = history_df.withColumn(
        "product_code", F.substring("article_id", 1, 7)
    )
    user_pc = history_pc.select("customer_id", "product_code").dropDuplicates()

    arts = articles_df.select(
        F.substring("article_id", 1, 7).alias("product_code"),
        "article_id",
    )

    siblings = user_pc.join(arts, "product_code", "inner")
    w = Window.partitionBy("customer_id").orderBy(F.col("article_id").desc())

    return (
        siblings.select("customer_id", "article_id").dropDuplicates()
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") <= top_n)
        .select("customer_id", "article_id")
        .withColumn("strategy", F.lit("sibling_product"))
    )


def main(run_date: str | None):
    spark = get_spark("CandidateSibling", driver_mem="8g")
    transactions = spark.read.parquet(PATHS["transactions"])
    articles = spark.read.parquet(PATHS["articles"])

    win = get_time_windows(transactions, HISTORY_DAYS, run_date)
    train_hist = filter_window(transactions, win["train_hist_start"], win["val_start"])
    test_hist = filter_window(transactions, win["test_hist_start"], win["test_start"])

    write_parquet(generate_sibling_candidates(train_hist, articles),
                  f"{PATHS['candidates']}/train_sibling.parquet")
    test_cands = generate_sibling_candidates(test_hist, articles)
    write_parquet(test_cands, f"{PATHS['candidates']}/test_sibling.parquet")

    evaluate_recall(test_cands, transactions, win["test_start"], win["max_date"], "Sibling test")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None)
    main(p.parse_args().run_date)
