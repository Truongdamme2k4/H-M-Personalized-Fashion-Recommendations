"""
Huấn luyện LightGBM ranking trên train_enriched.
Không cần Spark — đọc parquet bằng pandas/pyarrow, train trong driver.
Lưu model về HDFS thông qua hdfs CLI hoặc volume mount.
"""
import argparse
import gc
import os
import subprocess
import pandas as pd
import lightgbm as lgb

from common import PATHS


FEATURES = [
    "age", "item_total_sales", "item_avg_price",
    "user_total_purchases", "user_avg_budget", "user_item_buy_count",
    "days_since_last_purchase", "days_since_bought_THIS_item",
    "item_sales_last_3d", "item_sales_last_7d", "item_sales_last_14d",
    "trend_velocity", "age_group_item_sales", "user_type_buy_count",
    "age_diff", "price_diff",
    "from_als", "from_itemcf", "als_score", "itemcf_score",
    "product_type_name", "colour_group_name",
]
CATEGORICAL = ["product_type_name", "colour_group_name"]

LOCAL_MODEL = "/tmp/models/lightgbm_model.txt"
HDFS_MODEL = f"{PATHS['models']}/lightgbm_model.txt"


def downcast(df):
    for col in df.select_dtypes("float64").columns:
        df[col] = df[col].astype("float32")
    for col in df.select_dtypes("int64").columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")
    return df


def _strip_file_scheme(p: str) -> str:
    return p[len("file://"):] if p.startswith("file://") else p


def hdfs_to_local(hdfs_path: str, local_dir: str):
    """Nếu HDFS_BASE là file://, đọc trực tiếp filesystem; ngược lại dùng hdfs CLI."""
    if hdfs_path.startswith("file://"):
        return _strip_file_scheme(hdfs_path)
    os.makedirs(local_dir, exist_ok=True)
    subprocess.run(["hdfs", "dfs", "-get", "-f", hdfs_path, local_dir], check=True)
    return os.path.join(local_dir, os.path.basename(hdfs_path.rstrip("/")))


def local_to_hdfs(local_path: str, hdfs_path: str):
    if hdfs_path.startswith("file://"):
        target = _strip_file_scheme(hdfs_path)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        if local_path != target:
            subprocess.run(["cp", "-rf", local_path, target], check=True)
        return
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(hdfs_path)], check=True)
    subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path], check=True)


def main():
    train_path = hdfs_to_local(f"{PATHS['master']}/train_enriched.parquet", "/tmp/train_enriched")
    cols = FEATURES + ["label", "customer_id"]
    pdf = pd.read_parquet(train_path, columns=cols)
    pdf = downcast(pdf)
    for col in CATEGORICAL:
        pdf[col] = pdf[col].astype("category")

    X = pdf[FEATURES]
    y = pdf["label"]
    del pdf; gc.collect()

    dtrain = lgb.Dataset(X, label=y, categorical_feature=CATEGORICAL, free_raw_data=True)
    del X, y; gc.collect()

    params = {
        "objective": "binary", "metric": "auc", "n_estimators": 400,
        "learning_rate": 0.03, "num_leaves": 63, "max_depth": 8,
        "scale_pos_weight": 10.0, "min_child_samples": 100,
        "subsample": 0.8, "colsample_bytree": 0.8,
        "random_state": 42, "n_jobs": -1, "max_bin": 127,
    }
    booster = lgb.train(params, dtrain, callbacks=[lgb.log_evaluation(50)])

    os.makedirs(os.path.dirname(LOCAL_MODEL), exist_ok=True)
    booster.save_model(LOCAL_MODEL)
    local_to_hdfs(LOCAL_MODEL, HDFS_MODEL)

    importance = pd.DataFrame({
        "feature": booster.feature_name(),
        "gain": booster.feature_importance("gain"),
    }).sort_values("gain", ascending=False)
    print(importance.to_string(index=False))


if __name__ == "__main__":
    argparse.ArgumentParser().parse_args()
    main()
