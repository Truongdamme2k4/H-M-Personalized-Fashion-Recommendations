"""
Candidate generator: Item-based Collaborative Filtering qua co-occurrence.
Sinh Top-20 ứng viên + itemcf_score chuẩn hoá Min-Max per user.
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
TOP_N = 20
SIMILAR_PER_ITEM = 20


def generate_itemcf_candidates(history_df, top_n: int = TOP_N):
    user_item = history_df.select("customer_id", "article_id").dropDuplicates()

    pair_df = user_item.alias("i1").join(
        user_item.alias("i2"),
        F.col("i1.customer_id") == F.col("i2.customer_id")
    ).filter(F.col("i1.article_id") < F.col("i2.article_id"))

    co_occur = pair_df.groupBy(
        F.col("i1.article_id").alias("item_A"),
        F.col("i2.article_id").alias("item_B"),
    ).agg(F.count("*").alias("score"))

    co_occur_sym = co_occur.select(
        F.col("item_A").alias("item1"), F.col("item_B").alias("item2"), "score"
    ).union(co_occur.select(
        F.col("item_B").alias("item1"), F.col("item_A").alias("item2"), "score"
    ))

    w_item = Window.partitionBy("item1").orderBy(F.col("score").desc())
    sim_items = co_occur_sym.withColumn("rn", F.row_number().over(w_item)) \
        .filter(F.col("rn") <= SIMILAR_PER_ITEM).drop("rn")

    candidates = (
        user_item.withColumnRenamed("article_id", "item1")
        .join(sim_items, "item1")
        .groupBy("customer_id", "item2")
        .agg(F.sum("score").alias("itemcf_score_raw"))
    )

    w_user = Window.partitionBy("customer_id").orderBy(F.col("itemcf_score_raw").desc())
    top_cands = candidates.withColumn("rn", F.row_number().over(w_user)) \
        .filter(F.col("rn") <= top_n)

    w_scale = Window.partitionBy("customer_id")
    return (
        top_cands
        .withColumn("max_score", F.max("itemcf_score_raw").over(w_scale))
        .withColumn("min_score", F.min("itemcf_score_raw").over(w_scale))
        .withColumn(
            "itemcf_score",
            F.when(F.col("max_score") == F.col("min_score"), 1.0)
             .otherwise((F.col("itemcf_score_raw") - F.col("min_score")) /
                        (F.col("max_score") - F.col("min_score")))
        )
        .select("customer_id", F.col("item2").alias("article_id"), "itemcf_score")
        .withColumn("strategy", F.lit("itemcf"))
    )


def main(run_date: str | None):
    spark = get_spark("CandidateItemCF", driver_mem="10g")
    transactions = spark.read.parquet(PATHS["transactions"])

    win = get_time_windows(transactions, HISTORY_DAYS, run_date)
    train_hist = filter_window(transactions, win["train_hist_start"], win["val_start"])
    test_hist = filter_window(transactions, win["test_hist_start"], win["test_start"])

    write_parquet(generate_itemcf_candidates(train_hist),
                  f"{PATHS['candidates']}/train_itemcf.parquet")
    test_cands = generate_itemcf_candidates(test_hist)
    write_parquet(test_cands, f"{PATHS['candidates']}/test_itemcf.parquet")

    evaluate_recall(test_cands, transactions, win["test_start"], win["max_date"], "ItemCF test")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None)
    main(p.parse_args().run_date)
