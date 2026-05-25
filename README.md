# H&M Personalized Fashion Recommendations

Hệ thống gợi ý thời trang cá nhân hoá end-to-end trên dataset Kaggle **H&M Personalized Fashion Recommendations**, gồm:

- **Pipeline ETL** Spark + Airflow sinh Top-12 sản phẩm/khách hàng (6 nguồn ứng viên → LightGBM xếp hạng)
- **Backend** Node.js / Express phục vụ API gợi ý
- **Frontend** React (Vite + Mantine) hiển thị sản phẩm
- **MongoDB** lưu kết quả gợi ý cho backend truy vấn

## Cấu trúc

```
.
├── bigdata/                       # ETL pipeline (Spark + Airflow + Mongo)
│   ├── apps/                      # Spark scripts + helper Python
│   │   ├── common.py              # SparkSession builder + đường dẫn HDFS/local
│   │   ├── sample_dataset.py      # Sinh demo subset từ CSV gốc
│   │   ├── step1_cleaning.py      # Cleaning raw CSV → parquet
│   │   ├── candidate_*.py         # 6 nguồn ứng viên
│   │   ├── union_master.py        # Gộp ứng viên
│   │   ├── feature_label.py       # 22 đặc trưng + nhãn
│   │   ├── train_lightgbm.py      # Train LightGBM ranking
│   │   ├── predict_lightgbm.py    # Top-12 + fallback time-decayed
│   │   └── export_to_mongo.py     # Đẩy vào MongoDB
│   ├── dags/recsys_pipeline.py    # Airflow DAG
│   ├── docker-compose.yml         # Stack demo (single-node)
│   ├── docker-file.airflow.demo   # Airflow image custom
│   └── docker-stack.yml           # Phiên bản Swarm + Hadoop (production)
├── notebooks/                     # Notebook gốc trên Colab (Spark + Drive)
│   ├── candidates/                # 6 nguồn ứng viên
│   └── models/                    # LightGBM, FPGrowth, CLIP
├── backend/                       # Node.js Express API
└── frontend/                      # React + Vite UI
```

## Quick start — Demo pipeline end-to-end

### 1. Chuẩn bị data

Tải dataset Kaggle về (3 zip: `articles.csv.zip`, `customers.csv.zip`, `transactions_train.csv.zip`) và đặt trong project root. Sau đó giải nén:

```bash
mkdir -p data/raw
for f in articles.csv.zip customers.csv.zip transactions_train.csv.zip; do
  unzip -o "$f" -d data/raw/
done
```

### 2. Sinh demo subset (~30MB, 12K users)

```bash
python3 -m venv .demo_venv
.demo_venv/bin/pip install pandas pyarrow

.demo_venv/bin/python bigdata/apps/sample_dataset.py \
    --raw_dir data/raw \
    --out_dir data/demo \
    --user_frac 0.02 \
    --since 2020-06-01

# Copy demo vào data/raw để pipeline đọc
cp data/demo/*.csv data/raw/
```

### 3. Khởi động stack

```bash
cd bigdata
docker compose up -d --build
```

Đợi ~30s để Airflow init xong, rồi mở:

| URL | Service | Login |
|---|---|---|
| http://localhost:8082 | Airflow UI | admin / admin |
| http://localhost:8080 | Spark Master UI | — |
| http://localhost:8081 | Spark Worker UI | — |
| mongodb://localhost:27017 | MongoDB (`hm_recsys` db) | — |

### 4. Trigger DAG

Trên Airflow UI:
1. Bật toggle `recsys_pipeline_v1`
2. Click ▶️ → **Trigger DAG**
3. Xem tab **Graph** để theo dõi 14 task

Hoặc CLI:
```bash
docker compose exec airflow airflow dags unpause recsys_pipeline_v1
docker compose exec airflow airflow dags trigger recsys_pipeline_v1
```

Pipeline mất ~10 phút trên demo subset. Khi xong, kiểm tra Mongo:

```bash
docker compose exec mongodb mongosh hm_recsys --eval '
  db.user_recommendations.findOne();
  print(db.user_recommendations.countDocuments() + " users");
'
```

## Luồng pipeline

```
wait_raw_data (FileSensor)
   └→ step1_cleaning (Spark)
        └→ [6 candidate scripts song song] (Spark)
             ├ candidate_repurchase    — Top-15 items mới mua gần nhất
             ├ candidate_popularity    — Top-30 bestseller 7 ngày
             ├ candidate_sibling       — Các biến thể cùng product_code
             ├ candidate_als           — Spark ML ALS (rank=32, implicit)
             ├ candidate_itemcf        — Co-occurrence + cosine
             └ candidate_categorical   — Gu (gender × group × colour)
                  └→ union_master (Spark)
                       └→ feature_label (Spark, 22 features)
                            └→ train_lightgbm (Python/lightgbm)
                                 └→ predict_lightgbm — Top-12 + fallback
                                      └→ export_to_mongo — đẩy lên MongoDB
                                           └→ notify_done
```

### Recall các nguồn ứng viên (demo subset)

| Strategy | Top-N | Recall@test |
|---|---|---|
| ALS | 40 | 0.0323 |
| Categorical | 40 | 0.0292 |
| ItemCF | 20 | 0.0290 |
| Sibling | 15 | 0.0276 |
| Repurchase | 15 | 0.0254 |
| Popularity | 30 | 0.0236 |
| **Master (union)** | ~97 / user | **0.0905** |

## MongoDB schema

```javascript
// user_recommendations
{ _id: "<customer_id>", items: ["0817354001", ...], updated_at: ISODate }

// global_trending
{ _id: "global", items: [...top 12...], updated_at: ISODate }

// age_bestsellers
{ _id: "25-35" | "36-45" | ..., items: [...top 12...], updated_at: ISODate }

// pipeline_runs
{ run_date: "2020-09-22", finished_at: ISODate, user_count, global_count, age_groups }
```

## Stack components

| Service | Image | Port | Vai trò |
|---|---|---|---|
| airflow | `airflow-recsys:demo` (custom) | 8082 | Orchestrator + scheduler + webserver |
| spark-master | `bitnamilegacy/spark:3.5` | 8080, 7077 | Spark cluster master |
| spark-worker | `bitnamilegacy/spark:3.5` | 8081 | Spark executor (4GB / 4 cores) |
| mongodb | `mongo:6.0` | 27017 | Lưu recommendations |
| postgres | `postgres:13` | — | Airflow metadata DB |

Image airflow build từ `apache/airflow:2.10.4-python3.12` + `pyspark==3.5.6` + `lightgbm==4.1.0` + `pymongo==4.6.1` + `libgomp1` (LGBM runtime).

## Notes về thiết kế

### Tại sao bỏ Hadoop trong demo compose?

`docker-stack.yml` gốc có Hadoop (namenode, datanode, YARN) cho production. Với demo:
- Data nhỏ (~30MB) không cần HDFS
- Hadoop trên ARM64 (M-series Mac) chỉ chạy qua emulation, rất chậm
- Toàn bộ scripts đã hỗ trợ env `HDFS_BASE=file:///workspace` → đọc/ghi local FS

Production deploy dùng `docker stack deploy -c docker-stack.yml` (cần Docker Swarm), set `HDFS_BASE=hdfs://namenode:9000`.

### Tại sao hardcode `--run_date 2020-09-22`?

Dataset H&M dừng ở 2020-09-22. Nếu để `{{ ds }}` (ngày Airflow run), pipeline filter ra rỗng → ALS lỗi `No ratings available`. Production thay bằng `{{ ds }}` khi data refresh hàng ngày.

### Tại sao Python phải khớp giữa driver và worker?

PySpark serialize task qua Pickle, requires **exact minor version match**. Bitnami Spark 3.5 ship Python 3.12 → airflow image cũng phải Python 3.12 (`apache/airflow:2.10.4-python3.12`).

## Troubleshooting

| Lỗi | Nguyên nhân | Fix |
|---|---|---|
| `Could not parse Master URL: 'spark-master:7077'` | Conn `spark_default` thiếu scheme | Set `AIRFLOW_CONN_SPARK_DEFAULT` dạng JSON với `host: "spark://spark-master"` |
| `local class incompatible: serialVersionUID = ...` | pyspark version ≠ cluster version | Đồng bộ `pyspark` trong airflow image với `spark-submit --version` của cluster |
| `Python in worker has different version (3,X) than driver` | Airflow Python ≠ Spark Python | Dùng airflow image cùng Python minor version với Spark image |
| `No ratings available from MapPartitionsRDD` (ALS) | Filter time window không có data | Hardcode `--run_date` về ngày có data, hoặc backfill |
| `libgomp.so.1: cannot open shared object file` | LightGBM thiếu OpenMP runtime | Cài `libgomp1` vào image |
| Task `step1_cleaning` `up_for_retry` mãi | Spark master URL sai hoặc conn chưa load | Restart `airflow` container sau khi sửa env |

Logs từng task: Airflow UI → click task → tab **Logs**, hoặc:
```bash
docker compose exec airflow find /opt/airflow/logs/dag_id=recsys_pipeline_v1 -name '*.log' | tail
```

## Notebook gốc

Các notebook trong `notebooks/` được viết để chạy trực tiếp trên **Google Colab** (mount Drive, đường dẫn `/content/drive/MyDrive/HM-DATA/`). Là phiên bản research; scripts trong `bigdata/apps/` là port production-ready của chúng.

Notebook `FPGrowth.ipynb` (luật kết hợp) và `model CLIP.ipynb` (similar-products theo ảnh) chưa được wire vào DAG — có thể chạy riêng trên Colab hoặc đóng gói thành DAG phụ.

## Tham khảo

- Dataset: https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations
- Airflow Spark provider: https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/
- LightGBM ranking: https://lightgbm.readthedocs.io/
