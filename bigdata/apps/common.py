"""
Shared utilities cho recsys pipeline: Spark builder, đường dẫn HDFS,
chia khung thời gian train/test, evaluation recall.
"""
from __future__ import annotations
import os
import datetime

# pyspark imports lazy — cho phép pure-Python scripts (train/predict/export)
# import PATHS từ module này mà không cần pyspark.
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except ImportError:
    SparkSession = None
    F = None


HDFS_BASE = os.environ.get("HDFS_BASE", "hdfs://namenode:9000")

PATHS = {
    "transactions": f"{HDFS_BASE}/data/cleaned/transactions",
    "articles":     f"{HDFS_BASE}/data/cleaned/articles",
    "customers":    f"{HDFS_BASE}/data/cleaned/customers",
    "candidates":   f"{HDFS_BASE}/data/candidates",
    "master":       f"{HDFS_BASE}/data/master",
    "predictions":  f"{HDFS_BASE}/data/predictions",
    "models":       f"{HDFS_BASE}/data/models",
}

TRAIN_TARGET_DAYS = 7
TEST_TARGET_DAYS = 7
DEFAULT_HISTORY_DAYS = 42


def get_spark(app_name: str, driver_mem: str = "8g") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", driver_mem)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def get_time_windows(transactions, history_days: int = DEFAULT_HISTORY_DAYS,
                     run_date: str | None = None):
    """
    Trả về dict các mốc thời gian:
      max_date, test_start, val_start, train_hist_start, test_hist_start.

    run_date (YYYY-MM-DD) cho phép Airflow backfill — nếu None thì lấy max ngày
    có trong bảng transactions (lệch theo dataset, thuận tiện khi chạy local).
    """
    if run_date:
        max_date = datetime.date.fromisoformat(run_date)
    else:
        max_date = transactions.select(F.max("t_dat")).collect()[0][0]

    test_start = max_date - datetime.timedelta(days=TEST_TARGET_DAYS)
    val_start = test_start - datetime.timedelta(days=TRAIN_TARGET_DAYS)
    train_hist_start = val_start - datetime.timedelta(days=history_days)
    test_hist_start = test_start - datetime.timedelta(days=history_days)

    return {
        "max_date": max_date,
        "test_start": test_start,
        "val_start": val_start,
        "train_hist_start": train_hist_start,
        "test_hist_start": test_hist_start,
    }


def filter_window(transactions, start_date, end_date):
    return transactions.filter(
        (F.col("t_dat") >= start_date) & (F.col("t_dat") < end_date)
    )


def evaluate_recall(candidates_df, target_df, target_start, target_end, label: str = ""):
    actuals = filter_window(target_df, target_start, target_end) \
        .select("customer_id", "article_id").dropDuplicates()
    total = actuals.count()
    hits = actuals.join(candidates_df, ["customer_id", "article_id"], "inner") \
                  .dropDuplicates().count()
    recall = hits / total if total > 0 else 0.0
    print(f"[{label}] Actuals: {total:,} | Hits: {hits:,} | Recall: {recall:.4f}")
    return recall


def write_parquet(df, path: str):
    df.write.mode("overwrite").parquet(path)
    print(f"  Wrote: {path}")
