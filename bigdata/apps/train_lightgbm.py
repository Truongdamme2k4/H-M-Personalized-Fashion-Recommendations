"""
Huấn luyện LightGBM ranking trên train_enriched.
Không cần Spark — đọc parquet bằng pandas/pyarrow, train trong driver.
Lưu model về HDFS thông qua hdfs CLI hoặc volume mount.
"""
import argparse
import gc
import os
import pandas as pd
import lightgbm as lgb

from common import PATHS, s3a_to_local, local_to_s3a


FEATURES = [
    "age", "item_total_sales", "item_avg_price",
    "user_total_purchases", "user_avg_budget", "user_item_buy_count",
    "days_since_last_purchase", "days_since_bought_THIS_item",
    "item_sales_last_3d", "item_sales_last_7d", "item_sales_last_14d",
    "trend_velocity", "age_group_item_sales", "user_type_buy_count",
    "age_diff", "price_diff",
    "from_als", "from_itemcf", "from_fpgrowth",
    "als_score", "itemcf_score", "fpgrowth_score",
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


# Alias để các module khác (predict_lightgbm) tiếp tục import cùng tên
hdfs_to_local = s3a_to_local
local_to_hdfs = local_to_s3a


def main():
    train_path = s3a_to_local(f"{PATHS['master']}/train_enriched.parquet", "/tmp/train_enriched")
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
    local_to_s3a(LOCAL_MODEL, HDFS_MODEL)

    importance = pd.DataFrame({
        "feature": booster.feature_name(),
        "gain": booster.feature_importance("gain"),
    }).sort_values("gain", ascending=False)
    print(importance.to_string(index=False))


if __name__ == "__main__":
    argparse.ArgumentParser().parse_args()
    main()
