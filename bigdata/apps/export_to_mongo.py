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
import subprocess
import pandas as pd
from pymongo import MongoClient, UpdateOne, WriteConcern

from common import PATHS

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.environ.get("MONGO_DB", "hm_recsys")
BATCH_SIZE = 5000

LOCAL_TOP12 = "/tmp/top12_recommendations.parquet"
LOCAL_GLOBAL = "/tmp/global_top12.json"
LOCAL_AGE = "/tmp/age_bestsellers.json"


def hdfs_get(hdfs_path: str, local_path: str):
    if hdfs_path.startswith("file://"):
        return hdfs_path[len("file://"):]
    if os.path.exists(local_path):
        if os.path.isdir(local_path):
            subprocess.run(["rm", "-rf", local_path], check=True)
        else:
            os.remove(local_path)
    subprocess.run(["hdfs", "dfs", "-get", "-f", hdfs_path, local_path], check=True)
    return local_path


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
    top12_path  = hdfs_get(f"{PATHS['predictions']}/top12_recommendations.parquet", LOCAL_TOP12)
    global_path = hdfs_get(f"{PATHS['predictions']}/global_top12.json", LOCAL_GLOBAL)
    age_path    = hdfs_get(f"{PATHS['predictions']}/age_bestsellers.json", LOCAL_AGE)

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
    client.close()
    print("Export to Mongo done.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default=None)
    main(p.parse_args().run_date)
