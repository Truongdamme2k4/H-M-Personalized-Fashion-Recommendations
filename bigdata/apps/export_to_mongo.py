"""
Đọc Top-12 predictions từ HDFS, ghi vào MongoDB cho backend Node.js đọc.

Collections:
  - user_recommendations: {_id: customer_id, items: [...], updated_at}
  - global_trending:      {_id: 'global', items: [...], updated_at}
  - age_bestsellers:      {_id: <age_group>, items: [...], updated_at}
"""
from __future__ import annotations
import argparse
import datetime
import json
import os
import pandas as pd
from pymongo import MongoClient, UpdateOne, WriteConcern

from common import PATHS, s3a_to_local

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.environ.get("MONGO_DB", "hm_recsys")
BATCH_SIZE = 5000


def upsert_users(coll, top12_df: pd.DataFrame, now: datetime.datetime):
    ops = []
    written = 0
    for row in top12_df.itertuples(index=False):
        ops.append(UpdateOne(
            {"_id": row.customer_id},
            {"$set": {"items": list(row.predicted_items), "updated_at": now}},
            upsert=True,
        ))
        if len(ops) >= BATCH_SIZE:
            coll.bulk_write(ops, ordered=False)
            written += len(ops); ops = []
            print(f"  upserted {written:,} users")
    if ops:
        coll.bulk_write(ops, ordered=False)
        written += len(ops)
    print(f"  total users upserted: {written:,}")


def main(run_date: str | None):
    top12_path  = s3a_to_local(f"{PATHS['predictions']}/top12_recommendations.parquet", "/tmp/exp_top12")
    global_path = s3a_to_local(f"{PATHS['predictions']}/global_top12.json",              "/tmp/exp_global")
    age_path    = s3a_to_local(f"{PATHS['predictions']}/age_bestsellers.json",           "/tmp/exp_age")

    top12 = pd.read_parquet(top12_path)
    with open(global_path) as f:
        global_top = json.load(f)
    with open(age_path) as f:
        age_top = json.load(f)

    now = datetime.datetime.utcnow()
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    print(f"Connecting Mongo: {MONGO_URI}/{MONGO_DB}")
    print(f"Total users: {len(top12):,}")

    # User recommendations
    user_coll = db.get_collection("user_recommendations",
                                   write_concern=WriteConcern(w=1))
    upsert_users(user_coll, top12, now)

    # Global trending
    db["global_trending"].update_one(
        {"_id": "global"},
        {"$set": {"items": global_top, "updated_at": now}},
        upsert=True,
    )
    print(f"  global_trending: {len(global_top)} items")

    # Age-group bestsellers
    age_coll = db["age_bestsellers"]
    age_ops = [
        UpdateOne({"_id": ag}, {"$set": {"items": items, "updated_at": now}}, upsert=True)
        for ag, items in age_top.items()
    ]
    if age_ops:
        age_coll.bulk_write(age_ops, ordered=False)
    print(f"  age_bestsellers: {len(age_top)} groups")

    # Run metadata
    db["pipeline_runs"].insert_one({
        "run_date": run_date or str(datetime.date.today()),
        "finished_at": now,
        "user_count": len(top12),
        "global_count": len(global_top),
        "age_groups": list(age_top.keys()),
    })

    # Cart recommendations (từ FPGrowth — item-to-item co-purchase, không qua union)
    cart_path = os.environ.get("CART_REC_PATH", None)
    if cart_path:
        local_cart = s3a_to_local(cart_path, "/tmp/exp_cart")
        with open(local_cart) as f:
            cart_data = json.load(f)

        cart_coll = db["cart_recommendations"]
        cart_ops = [
            UpdateOne({"_id": aid}, {"$set": {"items": items}}, upsert=True)
            for aid, items in cart_data.items()
        ]
        if cart_ops:
            cart_coll.bulk_write(cart_ops, ordered=False)
        print(f"  cart_recommendations: {len(cart_data):,} articles")

    client.close()
    print("Export to Mongo done.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None)
    main(p.parse_args().run_date)
