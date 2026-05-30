"""
Shared utilities cho recsys pipeline: Spark builder (có s3a + JDBC),
đường dẫn MinIO datalake, watermark, time-window, recall eval.
"""
from __future__ import annotations
import os
import datetime
import json

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except ImportError:
    SparkSession = None
    F = None


# ===== Data lake =====
DATALAKE_BASE   = os.environ.get("DATALAKE_BASE",   "s3a://datalake")
MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS    = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

# Bronze = raw snapshot từ OLTP; Silver = cleaned; Gold = predictions/models
PATHS = {
    # silver (cleaned)
    "transactions": f"{DATALAKE_BASE}/silver/transactions",
    "articles":     f"{DATALAKE_BASE}/silver/articles",
    "customers":    f"{DATALAKE_BASE}/silver/customers",
    # bronze (raw OLTP snapshot)
    "bronze_transactions": f"{DATALAKE_BASE}/bronze/transactions",
    "bronze_articles":     f"{DATALAKE_BASE}/bronze/articles",
    "bronze_customers":    f"{DATALAKE_BASE}/bronze/customers",
    # candidates (silver) — file-name suffix khớp với code cũ
    "candidates":   f"{DATALAKE_BASE}/silver/candidates",
    "master":       f"{DATALAKE_BASE}/silver/master",
    # gold
    "predictions":  f"{DATALAKE_BASE}/gold/predictions",
    "models":       f"{DATALAKE_BASE}/gold/models",
    # _state (watermarks etc)
    "state":        f"{DATALAKE_BASE}/_state",
}

# ===== OLTP JDBC =====
OLTP_JDBC_URL   = os.environ.get("OLTP_JDBC_URL", "jdbc:postgresql://oltp-postgres:5432/hm_oltp")
OLTP_JDBC_USER  = os.environ.get("OLTP_JDBC_USER", "hm")
OLTP_JDBC_PASS  = os.environ.get("OLTP_JDBC_PASSWORD", "hm")

# Spark packages cần kéo về (s3a + postgres jdbc)
SPARK_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.5.4",
])

TRAIN_TARGET_DAYS = 7
TEST_TARGET_DAYS = 7
DEFAULT_HISTORY_DAYS = 42


def get_spark(app_name: str, driver_mem: str = "8g") -> SparkSession:
    """Spark session với cấu hình s3a (MinIO) + JDBC packages."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", driver_mem)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def get_time_windows(transactions, history_days: int = DEFAULT_HISTORY_DAYS,
                     run_date: str | None = None):
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


# ===== Watermark helpers (pure Python, dùng boto3 không cần Spark) =====
def _boto_client():
    import boto3
    from botocore.client import Config
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _state_bucket_key(name: str):
    base = DATALAKE_BASE.replace("s3a://", "").replace("s3://", "")
    parts = base.split("/", 1)
    bucket = parts[0]
    prefix = (parts[1] + "/" if len(parts) > 1 else "") + "_state/"
    return bucket, f"{prefix}{name}.json"


def read_watermark(name: str, default: str = "1900-01-01") -> str:
    bucket, key = _state_bucket_key(name)
    try:
        obj = _boto_client().get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))["value"]
    except Exception:
        return default


def write_watermark(name: str, value: str):
    bucket, key = _state_bucket_key(name)
    body = json.dumps({"value": value, "updated_at": datetime.datetime.utcnow().isoformat()}).encode("utf-8")
    _boto_client().put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    print(f"  Watermark {name} → {value}")


# ===== S3A <-> local helpers (cho task pure-Python: train/predict/export) =====
def _split_s3a(s3a_uri: str):
    """s3a://bucket/key → (bucket, key)"""
    p = s3a_uri.replace("s3a://", "").replace("s3://", "")
    parts = p.split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def s3a_to_local(s3a_path: str, local_dir: str) -> str:
    """Download object or prefix-tree from MinIO về local. Trả về đường dẫn local."""
    import os as _os
    s3 = _boto_client()
    bucket, key = _split_s3a(s3a_path)
    _os.makedirs(local_dir, exist_ok=True)

    # Liệt kê objects dưới prefix (dir-style)
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=key.rstrip("/") + "/")
    files = []
    for page in pages:
        for obj in page.get("Contents", []) or []:
            files.append(obj["Key"])

    if files:
        # Là "dir" — download cả thư mục giữ nguyên cấu trúc tương đối
        target_root = _os.path.join(local_dir, _os.path.basename(key.rstrip("/")))
        for k in files:
            rel = k[len(key.rstrip("/")) + 1:]
            target = _os.path.join(target_root, rel)
            _os.makedirs(_os.path.dirname(target), exist_ok=True)
            s3.download_file(bucket, k, target)
        return target_root

    # Là single object
    target = _os.path.join(local_dir, _os.path.basename(key))
    s3.download_file(bucket, key, target)
    return target


def local_to_s3a(local_path: str, s3a_path: str):
    """Upload file/dir local lên MinIO."""
    import os as _os
    s3 = _boto_client()
    bucket, key = _split_s3a(s3a_path)

    if _os.path.isdir(local_path):
        for root, _, fnames in _os.walk(local_path):
            for fn in fnames:
                full = _os.path.join(root, fn)
                rel = _os.path.relpath(full, local_path)
                s3.upload_file(full, bucket, f"{key.rstrip('/')}/{rel}")
    else:
        s3.upload_file(local_path, bucket, key)
    print(f"  Uploaded → s3a://{bucket}/{key}")
