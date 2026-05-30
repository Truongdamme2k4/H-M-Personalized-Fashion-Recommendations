"""
Predict buy_prob cho test_enriched theo batch, lấy Top-12/user và bù
fallback time-decayed bestseller theo age_group cho user thiếu ứng viên.
Output:
  - test_predictions_lgbm.parquet (customer_id, article_id, buy_prob)
  - top12_recommendations.parquet (customer_id, predicted_items: list<string>)
  - global_top12.json (12 sản phẩm bestseller toàn cục)
  - age_bestsellers.json (Top-12 theo nhóm tuổi)
"""
from __future__ import annotations
import argparse
import datetime
import gc
import json
import math
import os
import subprocess
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import lightgbm as lgb

from train_lightgbm import (
    FEATURES, CATEGORICAL, hdfs_to_local, local_to_hdfs,
    LOCAL_MODEL, HDFS_MODEL,
)
from common import PATHS

DECAY_FACTOR = 0.95
DECAY_WINDOW_DAYS = 21
TOP_K = 12

LOCAL_PREDS = "/tmp/test_predictions_lgbm.parquet"
HDFS_PREDS = f"{PATHS['predictions']}/test_predictions_lgbm.parquet"
HDFS_TOP12 = f"{PATHS['predictions']}/top12_recommendations.parquet"
HDFS_GLOBAL = f"{PATHS['predictions']}/global_top12.json"
HDFS_AGE = f"{PATHS['predictions']}/age_bestsellers.json"


def categorize_age(age):
    if pd.isna(age): return "Unknown"
    if age < 25: return "<25"
    if age <= 35: return "25-35"
    if age <= 45: return "36-45"
    if age <= 55: return "46-55"
    return ">55"


def batched_predict(booster, test_local_path: str):
    dataset = ds.dataset(test_local_path, format="parquet", ignore_prefixes=[".", "_"])
    cols = FEATURES + ["customer_id", "article_id"]
    writer = None
    for i, batch in enumerate(dataset.to_batches(columns=cols, batch_size=2_000_000), 1):
        df = batch.to_pandas()
        for col in df.select_dtypes("float64").columns:
            df[col] = df[col].astype("float32")
        for col in df.select_dtypes("int64").columns:
            df[col] = pd.to_numeric(df[col], downcast="integer")
        for col in CATEGORICAL:
            df[col] = df[col].astype("category")
        df["buy_prob"] = booster.predict(df[FEATURES])
        res = df[["customer_id", "article_id", "buy_prob"]]
        table = pa.Table.from_pandas(res)
        if writer is None:
            writer = pq.ParquetWriter(LOCAL_PREDS, table.schema)
        writer.write_table(table)
        print(f"  batch {i} ok")
        del df, res, table, batch; gc.collect()
    if writer:
        writer.close()


def compute_fallback_bestsellers(test_start: datetime.date):
    decay_start = test_start - datetime.timedelta(days=DECAY_WINDOW_DAYS)

    trans_local = hdfs_to_local(PATHS["transactions"], "/tmp/transactions")
    cust_local = hdfs_to_local(PATHS["customers"], "/tmp/customers")

    trend_df = pd.read_parquet(
        trans_local, columns=["t_dat", "customer_id", "article_id"],
        filters=[("t_dat", ">=", decay_start), ("t_dat", "<", test_start)],
    )
    ts = pd.to_datetime(test_start)
    trend_df["days_ago"] = (ts - pd.to_datetime(trend_df["t_dat"])).dt.days
    trend_df["weight"] = DECAY_FACTOR ** trend_df["days_ago"]

    global_top = trend_df.groupby("article_id")["weight"].sum() \
        .nlargest(TOP_K).index.tolist()

    cust = pd.read_parquet(cust_local, columns=["customer_id", "age"])
    cust["age_group"] = cust["age"].apply(categorize_age)
    trend_df = trend_df.merge(cust[["customer_id", "age_group"]], on="customer_id", how="left")
    trend_df["age_group"] = trend_df["age_group"].fillna("Unknown")

    age_top = {}
    for ag, grp in trend_df.groupby("age_group"):
        age_top[str(ag)] = grp.groupby("article_id")["weight"].sum() \
            .nlargest(TOP_K).index.tolist()
    if "Unknown" not in age_top or len(age_top["Unknown"]) < TOP_K:
        age_top["Unknown"] = global_top

    return global_top, age_top, cust[["customer_id", "age_group"]]


def assemble_top12(global_top, age_top, customers_age):
    preds = pd.read_parquet(LOCAL_PREDS)
    preds = preds.sort_values(["customer_id", "buy_prob", "article_id"],
                              ascending=[True, False, True])
    top12 = preds.groupby("customer_id").head(TOP_K) \
        .groupby("customer_id")["article_id"].apply(list) \
        .reset_index(name="predicted_items")
    del preds; gc.collect()

    top12 = top12.merge(customers_age, on="customer_id", how="left")
    top12["age_group"] = top12["age_group"].fillna("Unknown")

    def fill(row):
        items = row["predicted_items"] or []
        if len(items) >= TOP_K:
            return items[:TOP_K]
        fallback = age_top.get(row["age_group"], global_top)
        filler = [it for it in fallback if it not in items][: TOP_K - len(items)]
        if len(items) + len(filler) < TOP_K:
            extra = [it for it in global_top if it not in items and it not in filler]
            filler.extend(extra[: TOP_K - len(items) - len(filler)])
        return items + filler

    top12["predicted_items"] = top12.apply(fill, axis=1)
    return top12[["customer_id", "predicted_items"]]


def upload_json(obj, hdfs_path: str):
    tmp = f"/tmp/{os.path.basename(hdfs_path)}"
    with open(tmp, "w") as f:
        json.dump(obj, f)
    local_to_hdfs(tmp, hdfs_path)


def main(run_date: str | None):
    model_path = hdfs_to_local(HDFS_MODEL, os.path.dirname(LOCAL_MODEL))
    booster = lgb.Booster(model_file=model_path)

    test_local = hdfs_to_local(f"{PATHS['master']}/test_enriched.parquet", "/tmp/test_enriched")

    print("Predicting in batches...")
    batched_predict(booster, test_local)
    local_to_hdfs(LOCAL_PREDS, HDFS_PREDS)

    # Determine test_start
    trans_local = hdfs_to_local(PATHS["transactions"], "/tmp/transactions")
    if run_date:
        max_date = datetime.date.fromisoformat(run_date)
    else:
        dates = pd.read_parquet(trans_local, columns=["t_dat"])
        max_date = dates["t_dat"].max()
        if isinstance(max_date, pd.Timestamp):
            max_date = max_date.date()
    test_start = max_date - datetime.timedelta(days=7)

    print("Computing fallback bestsellers...")
    global_top, age_top, cust_age = compute_fallback_bestsellers(test_start)

    print("Assembling Top-12 with fallback...")
    top12 = assemble_top12(global_top, age_top, cust_age)

    # Persist Top-12 + fallback metadata
    local_top12 = "/tmp/top12_recommendations.parquet"
    top12.to_parquet(local_top12, index=False)
    local_to_hdfs(local_top12, HDFS_TOP12)
    upload_json(global_top, HDFS_GLOBAL)
    upload_json(age_top, HDFS_AGE)
    print("Done.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None)
    main(p.parse_args().run_date)
