#!/usr/bin/env bash
# Demo pipeline runner: chạy toàn bộ pipeline trên local filesystem qua Docker spark.
# Yêu cầu: data/demo/{transactions_train,articles,customers}.csv đã sinh sẵn bởi sample_dataset.py
#
# Usage:  bash bigdata/apps/demo_run.sh
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DEMO_DIR="$PROJECT_ROOT/data/demo"
WORK_DIR="$PROJECT_ROOT/data"          # mount thành /work trong container
IMAGE="apache/spark-py:v3.4.0"

if [[ ! -f "$DEMO_DIR/transactions_train.csv" ]]; then
  echo "ERROR: $DEMO_DIR/transactions_train.csv không tồn tại."
  echo "Chạy trước: python bigdata/apps/sample_dataset.py ..."
  exit 1
fi

# Symlink demo CSV vào layout 'raw' mà step1 đọc
mkdir -p "$WORK_DIR/raw"
for f in transactions_train articles customers; do
  cp -f "$DEMO_DIR/$f.csv" "$WORK_DIR/raw/$f.csv"
done

mkdir -p "$WORK_DIR/cleaned" "$WORK_DIR/candidates" "$WORK_DIR/master" "$WORK_DIR/predictions" "$WORK_DIR/models"

run_spark() {
  local script="$1"; shift
  echo
  echo "=========================================================="
  echo ">>> Spark: $script $*"
  echo "=========================================================="
  docker run --rm \
    --user root \
    --network hm-demo-net \
    -v "$WORK_DIR:/workspace/data" \
    -v "$PROJECT_ROOT/bigdata/apps:/opt/spark/work-dir" \
    -v "$HOME/.cache/hm-demo-pip:/root/.cache/pip" \
    -w /opt/spark/work-dir \
    -e HDFS_BASE="file:///workspace" \
    -e PYTHONPATH=/opt/spark/work-dir \
    -e SPARK_LOCAL_IP=127.0.0.1 \
    -e SPARK_LOCAL_HOSTNAME=localhost \
    "$IMAGE" \
    bash -c "pip install --quiet numpy 2>/dev/null; \
      /opt/spark/bin/spark-submit \
        --master 'local[*]' \
        --driver-memory 4g \
        --conf spark.sql.shuffle.partitions=8 \
        --conf spark.driver.host=localhost \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.network.timeout=600s \
        '$script' $*"
}

run_python() {
  # Chạy script Python thuần (không Spark) — dùng cho lightgbm/mongo
  local script="$1"; shift
  echo
  echo "=========================================================="
  echo ">>> Python: $script $*"
  echo "=========================================================="
  docker run --rm \
    --user root \
    --network hm-demo-net \
    -v "$WORK_DIR:/workspace/data" \
    -v "$PROJECT_ROOT/bigdata/apps:/opt/spark/work-dir" \
    -v "$HOME/.cache/hm-demo-pip:/root/.cache/pip" \
    -w /opt/spark/work-dir \
    -e HDFS_BASE="file:///workspace" \
    -e PYTHONPATH=/opt/spark/work-dir \
    -e MONGO_URI="mongodb://hm-mongo:27017" \
    -e MONGO_DB="hm_recsys" \
    "$IMAGE" \
    bash -c "pip install --quiet lightgbm pymongo numpy 2>/dev/null; \
      python '$script' $*"
}

# Khởi tạo Docker network để Spark/Python container nói chuyện với Mongo
docker network inspect hm-demo-net >/dev/null 2>&1 || docker network create hm-demo-net >/dev/null

# Spin up Mongo nếu chưa chạy
if ! docker ps --format '{{.Names}}' | grep -q '^hm-mongo$'; then
  echo ">>> Starting MongoDB container 'hm-mongo'..."
  docker run -d --rm --name hm-mongo --network hm-demo-net -p 27017:27017 \
    -e MONGO_INITDB_DATABASE=hm_recsys mongo:6.0 >/dev/null
  echo "Wait Mongo ready..."
  sleep 5
fi

# Chạy lần lượt từng stage
run_spark step1_cleaning.py
run_spark candidate_repurchase.py
run_spark candidate_popularity.py
run_spark candidate_sibling.py
run_spark candidate_als.py
run_spark candidate_itemcf.py
run_spark candidate_categorical.py
run_spark union_master.py
run_spark feature_label.py
run_python train_lightgbm.py
run_python predict_lightgbm.py
run_python export_to_mongo.py

echo
echo "=========================================================="
echo "DEMO PIPELINE DONE — outputs trong $WORK_DIR"
echo "=========================================================="
ls -la "$WORK_DIR/candidates" "$WORK_DIR/master" "$WORK_DIR/predictions" 2>/dev/null
echo
echo "Mongo collections:"
docker exec hm-mongo mongosh --quiet hm_recsys --eval \
  'db.getCollectionNames().forEach(c => print("  " + c + ": " + db[c].countDocuments() + " docs"))'
