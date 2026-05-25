"""
Candidate generator: Categorical profile — gu thời trang theo tổ hợp
gender × product_group × colour, ghép user-top-combo với item-hot-combo.
"""
from __future__ import annotations
import argparse
import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (
    get_spark, get_time_windows, filter_window, write_parquet,
    evaluate_recall, PATHS,
)

HISTORY_DAYS = 90
TOP_N = 40
USER_TOP_COMBOS = 5
ITEMS_PER_COMBO = 20
TREND_DAYS = 14


def generate_categorical_candidates(history_df, articles_df, top_n: int = TOP_N):
    # Some clusters dùng tên 'gender' khác — fallback sang index_group_name nếu cần
    gender_col = "gender" if "gender" in articles_df.columns else "index_group_name"

    arts_meta = articles_df.select(
        "article_id", gender_col, "product_group_name", "colour_group_name"
    ).withColumn(
        "combo_id",
        F.concat_ws("_", F.col(gender_col), F.col("product_group_name"), F.col("colour_group_name")),
    )

    hist = history_df.join(arts_meta, "article_id", "inner")

    user_profile = hist.groupBy("customer_id", "combo_id") \
        .agg(F.count("*").alias("user_affinity"))
    w_uc = Window.partitionBy("customer_id").orderBy(F.col("user_affinity").desc())
    user_top = user_profile.withColumn("rn", F.row_number().over(w_uc)) \
        .filter(F.col("rn") <= USER_TOP_COMBOS).drop("rn")

    hist_max = history_df.select(F.max("t_dat")).collect()[0][0]
    recent_start = hist_max - datetime.timedelta(days=TREND_DAYS)
    recent_hist = hist.filter(F.col("t_dat") >= recent_start)

    combo_pop = recent_hist.groupBy("combo_id", "article_id") \
        .agg(F.count("*").alias("item_hotness"))
    w_ci = Window.partitionBy("combo_id").orderBy(F.col("item_hotness").desc())
    trending = combo_pop.withColumn("rn", F.row_number().over(w_ci)) \
        .filter(F.col("rn") <= ITEMS_PER_COMBO).drop("rn")

    candidates = user_top.join(trending, "combo_id", "inner") \
        .withColumn("lr_proxy_score", F.col("user_affinity") * F.col("item_hotness"))

    w_final = Window.partitionBy("customer_id").orderBy(F.col("lr_proxy_score").desc())
    return (
        candidates.withColumn("rn", F.row_number().over(w_final))
        .filter(F.col("rn") <= top_n)
        .select("customer_id", "article_id")
        .withColumn("strategy", F.lit("categorical_profile"))
    )


def main(run_date: str | None):
    spark = get_spark("CandidateCategorical", driver_mem="10g")
    transactions = spark.read.parquet(PATHS["transactions"])
    articles = spark.read.parquet(PATHS["articles"])

    win = get_time_windows(transactions, HISTORY_DAYS, run_date)
    train_hist = filter_window(transactions, win["train_hist_start"], win["val_start"])
    test_hist = filter_window(transactions, win["test_hist_start"], win["test_start"])

    write_parquet(generate_categorical_candidates(train_hist, articles),
                  f"{PATHS['candidates']}/train_categorical.parquet")
    test_cands = generate_categorical_candidates(test_hist, articles)
    write_parquet(test_cands, f"{PATHS['candidates']}/test_categorical.parquet")

    evaluate_recall(test_cands, transactions, win["test_start"], win["max_date"], "Categorical test")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None)
    main(p.parse_args().run_date)
