"""
Extract OLTP Postgres → MinIO bronze.
- articles, customers: full snapshot (ghi đè).
- transactions: incremental theo t_dat ≥ watermark (append).

Watermark lưu tại s3a://datalake/_state/transactions_t_dat.json
"""
from __future__ import annotations
import argparse
import datetime
from pyspark.sql import functions as F

from common import (
    get_spark, PATHS,
    OLTP_JDBC_URL, OLTP_JDBC_USER, OLTP_JDBC_PASS,
    read_watermark, write_watermark,
)


JDBC_OPTS = {
    "url": OLTP_JDBC_URL,
    "user": OLTP_JDBC_USER,
    "password": OLTP_JDBC_PASS,
    "driver": "org.postgresql.Driver",
}


def read_jdbc(spark, dbtable: str):
    return spark.read.format("jdbc").options(**JDBC_OPTS, dbtable=dbtable).load()


def extract_articles(spark):
    df = read_jdbc(spark, "articles")
    df.write.mode("overwrite").parquet(PATHS["bronze_articles"])
    print(f"  bronze/articles: {df.count():,} rows")


def extract_customers(spark):
    df = read_jdbc(spark, "customers")
    df.write.mode("overwrite").parquet(PATHS["bronze_customers"])
    print(f"  bronze/customers: {df.count():,} rows")


def extract_transactions_incremental(spark):
    last = read_watermark("transactions_t_dat", default="1900-01-01")
    print(f"  watermark (last t_dat) = {last}")

    query = f"(SELECT t_dat, customer_id, article_id, price, sales_channel_id " \
            f"FROM transactions WHERE t_dat > DATE '{last}') AS tx_inc"
    df = read_jdbc(spark, query)
    new_count = df.count()
    print(f"  new transactions: {new_count:,}")

    if new_count == 0:
        print("  Nothing to sync.")
        return

    df.write.mode("append").partitionBy("t_dat").parquet(PATHS["bronze_transactions"])
    new_max = df.select(F.max("t_dat")).collect()[0][0]
    write_watermark("transactions_t_dat", str(new_max))


def main():
    spark = get_spark("ExtractOLTPtoMinIO", driver_mem="4g")
    extract_articles(spark)
    extract_customers(spark)
    extract_transactions_incremental(spark)
    spark.stop()
    print("Extract OLTP → MinIO done.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.parse_args()
    main()
