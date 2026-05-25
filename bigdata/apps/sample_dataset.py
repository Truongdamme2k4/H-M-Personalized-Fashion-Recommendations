"""
Tạo demo subset từ Kaggle H&M dataset gốc.

Combo strategy: date window + user sampling
  - Cắt giao dịch >= --since (default 2020-06-01) để chỉ giữ ~3 tháng cuối
  - Sample --user_frac users ngẫu nhiên (default 0.02 = 2%)
  - Lọc articles + customers chỉ giữ những ID còn được tham chiếu

Usage:
  python sample_dataset.py \\
      --raw_dir  ~/Downloads/hm-dataset \\
      --out_dir  ~/hm-demo \\
      --user_frac 0.02 \\
      --since 2020-06-01

Sau đó upload lên HDFS:
  hdfs dfs -mkdir -p /data/raw
  hdfs dfs -put ~/hm-demo/*.csv /data/raw/
"""
from __future__ import annotations
import argparse
import os
import pandas as pd


def main(raw_dir: str, out_dir: str, user_frac: float, since: str, seed: int):
    os.makedirs(out_dir, exist_ok=True)

    print(f"[1/4] Đọc transactions_train.csv (file lớn nhất, ~3GB)...")
    transactions = pd.read_csv(
        os.path.join(raw_dir, "transactions_train.csv"),
        dtype={"article_id": str, "customer_id": str},
        parse_dates=["t_dat"],
    )
    print(f"  Raw: {len(transactions):,} rows | date {transactions['t_dat'].min().date()} → {transactions['t_dat'].max().date()}")

    print(f"\n[2/4] Lọc date >= {since}...")
    transactions = transactions[transactions["t_dat"] >= pd.Timestamp(since)]
    print(f"  Sau date filter: {len(transactions):,} rows")

    print(f"\n[3/4] Sample {user_frac:.1%} users + lấy toàn bộ giao dịch của họ...")
    all_users = transactions["customer_id"].drop_duplicates()
    sampled_users = all_users.sample(frac=user_frac, random_state=seed)
    print(f"  Users: {len(all_users):,} → {len(sampled_users):,}")

    transactions = transactions[transactions["customer_id"].isin(set(sampled_users))]
    print(f"  Transactions sau user sample: {len(transactions):,}")

    kept_article_ids = set(transactions["article_id"].unique())
    kept_customer_ids = set(transactions["customer_id"].unique())

    print(f"\n[4/4] Lọc articles + customers theo ID còn được tham chiếu...")
    articles = pd.read_csv(os.path.join(raw_dir, "articles.csv"), dtype={"article_id": str})
    articles = articles[articles["article_id"].isin(kept_article_ids)]
    print(f"  Articles: {len(articles):,}")

    customers = pd.read_csv(os.path.join(raw_dir, "customers.csv"), dtype={"customer_id": str})
    customers = customers[customers["customer_id"].isin(kept_customer_ids)]
    print(f"  Customers: {len(customers):,}")

    out_trans = os.path.join(out_dir, "transactions_train.csv")
    out_arts = os.path.join(out_dir, "articles.csv")
    out_cust = os.path.join(out_dir, "customers.csv")

    transactions.to_csv(out_trans, index=False, date_format="%Y-%m-%d")
    articles.to_csv(out_arts, index=False)
    customers.to_csv(out_cust, index=False)

    print("\n=== DONE ===")
    print(f"  {out_trans}  ({os.path.getsize(out_trans) / 1e6:.1f} MB)")
    print(f"  {out_arts}   ({os.path.getsize(out_arts) / 1e6:.1f} MB)")
    print(f"  {out_cust}   ({os.path.getsize(out_cust) / 1e6:.1f} MB)")
    print(f"\nUpload HDFS:\n  hdfs dfs -mkdir -p /data/raw")
    print(f"  hdfs dfs -put {out_dir}/*.csv /data/raw/")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--raw_dir", required=True, help="Thư mục chứa 3 CSV gốc từ Kaggle")
    p.add_argument("--out_dir", required=True, help="Thư mục output cho mini CSV")
    p.add_argument("--user_frac", type=float, default=0.02, help="Tỉ lệ user giữ lại (0.02 = 2%%)")
    p.add_argument("--since", default="2020-06-01", help="Ngày sớm nhất giữ lại (YYYY-MM-DD)")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    main(args.raw_dir, args.out_dir, args.user_frac, args.since, args.seed)
