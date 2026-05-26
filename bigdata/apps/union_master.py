"""
Gộp 6 nguồn candidates thành master, giữ điểm als_score / itemcf_score cao nhất
và collect strategies vào cột sources.
"""
from __future__ import annotations
import argparse
from pyspark.sql import functions as F

from common import get_spark, get_time_windows, evaluate_recall, write_parquet, PATHS


STRATEGIES = ["repurchase", "popularity", "sibling", "als", "itemcf", "categorical", "fpgrowth"]


def standardize(df):
    cols = df.columns
    if "als_score" not in cols:
        df = df.withColumn("als_score", F.lit(0.0))
    if "itemcf_score" not in cols:
        df = df.withColumn("itemcf_score", F.lit(0.0))
    if "fpgrowth_score" not in cols:
        df = df.withColumn("fpgrowth_score", F.lit(0.0))
    return df.select("customer_id", "article_id", "strategy",
                     "als_score", "itemcf_score", "fpgrowth_score")


def create_master(df_list):
    master = df_list[0]
    for df in df_list[1:]:
        master = master.unionByName(df)

    return (
        master.groupBy("customer_id", "article_id")
        .agg(
            F.collect_set("strategy").alias("sources"),
            F.max("als_score").alias("als_score"),
            F.max("itemcf_score").alias("itemcf_score"),
            F.max("fpgrowth_score").alias("fpgrowth_score"),
        )
        .withColumn("source_count", F.size("sources"))
    )


def load_all(spark, split: str):
    dfs = []
    for strat in STRATEGIES:
        path = f"{PATHS['candidates']}/{split}_{strat}.parquet"
        try:
            df = spark.read.parquet(path)
            dfs.append(standardize(df))
            print(f"  Loaded {split}: {strat}")
        except Exception as e:
            print(f"  WARN: skip {split}_{strat}: {e}")
    return dfs


def main(run_date: str | None):
    spark = get_spark("UnionMaster", driver_mem="10g")

    train_master = create_master(load_all(spark, "train"))
    write_parquet(train_master, f"{PATHS['master']}/train_master_candidates.parquet")

    test_master = create_master(load_all(spark, "test"))
    write_parquet(test_master, f"{PATHS['master']}/test_master_candidates.parquet")

    transactions = spark.read.parquet(PATHS["transactions"])
    win = get_time_windows(transactions, run_date=run_date)
    avg_cands = test_master.groupBy("customer_id").count() \
        .select(F.avg("count")).collect()[0][0]
    print(f"Average candidates per user: {avg_cands:.1f}")
    evaluate_recall(test_master, transactions, win["test_start"], win["max_date"], "MASTER test")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None)
    main(p.parse_args().run_date)
