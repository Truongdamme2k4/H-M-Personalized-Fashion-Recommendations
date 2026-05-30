# Báo cáo hệ thống Apache Airflow — Dự án H&M Personalized Fashion Recommendations

**Cập nhật:** 2026-05-30

---

# 1. Tổng quan

Dự án sử dụng **Apache Airflow 2.10** làm orchestration layer, điều phối toàn bộ pipeline gợi ý từ lúc đọc dữ liệu OLTP đến lúc export kết quả lên MongoDB.

DAG chính: `recsys_pipeline_v1`, định nghĩa trong `bigdata/dags/recsys_pipeline.py`.

---

# 2. Kiến trúc DAG

## 2.1. Luồng End-to-End

```
wait_oltp_ready
  → extract_oltp_to_minio           (Spark JDBC → MinIO bronze)
  → step1_cleaning                  (bronze → silver)
  → [7 candidate generators song song]
        cand_repurchase
        cand_popularity
        cand_sibling
        cand_als
        cand_itemcf
        cand_categorical
        export_cart_fpgrowth
  → union_master                    (fan-in: gộp 7 luồng)
  → feature_label                   (tính features + nhãn)
  → train_lightgbm                  (Bash → Python thuần)
  → predict_lightgbm                (Bash → Python thuần)
  → export_to_mongo                 (Bash → Python thuần, export MongoDB)
  → notify_done                     (log hoàn tất)
```

## 2.2. Đặc điểm cấu trúc

| Đặc điểm | Giá trị |
|---|---|
| Tổng số task | 15 |
| Fan-out | 7 candidate generators chạy song song |
| Fan-in | union_master gộp 7 luồng |
| schedule_interval | `None` (trigger thủ công cho demo; production → weekly) |
| max_active_runs | 1 |
| retries | 1 lần, retry_delay 5 phút |
| execution_timeout | 2 giờ |
| tags | `["recsys", "hm"]` |

---

# 3. Chi tiết từng Task

## 3.1. wait_oltp_ready — PythonSensor

**Mục đích:** Chờ PostgreSQL OLTP seed xong dữ liệu trước khi Spark đọc.

**Logic:** Kết nối tới `oltp-postgres:5432`, chạy `SELECT count(*) FROM transactions`. Pass nếu `n > 0`, fail nếu exception.

**Cấu hình:**
- `poke_interval=15` giây giữa các lần thử
- `timeout=600` giây (10 phút)
- `mode="reschedule"` — sensor không chiếm worker slot khi đợi

**Tại sao cần sensor:** Container `oltp-postgres` seed ~1-2 phút từ CSV. Không sensor thì Spark JDBC đọc bảng rỗng → pipeline fail ngay từ bước đầu.

## 3.2. extract_oltp_to_minio — SparkSubmitOperator

**Mục đích:** Copy ba bảng (articles, customers, transactions) từ PostgreSQL OLTP → MinIO bronze layer dưới dạng parquet.

**Script:** `bigdata/apps/extract_oltp_to_minio.py`

**Spark config truyền xuống:**

```python
SPARK_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",     # giao thức s3a
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.5.4",        # JDBC driver
])
SPARK_EXECUTOR_CONF = {
    "spark.master": "spark://spark-master:7077",
    "spark.jars.packages": SPARK_PACKAGES,
    **{f"spark.executorEnv.{k}": v for k, v in SPARK_ENV_VARS.items()},
}
```

**Env được truyền:** `DATALAKE_BASE`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `OLTP_JDBC_URL`, `OLTP_JDBC_USER`, `OLTP_JDBC_PASSWORD`, `PYTHONPATH`

**Tối ưu:** Dùng 8 JDBC partition song song để extract bảng 31 triệu dòng — giảm thời gian extract ~8 lần so với đọc tuần tự.

## 3.3. step1_cleaning — SparkSubmitOperator

**Mục đích:** Đọc bronze → làm sạch → ghi silver.

**Script:** `bigdata/apps/step1_cleaning.py`

**Các bước làm sạch:**
- Loại bỏ trùng theo khoá chính
- Điền giá trị mặc định: tuổi → 25, club_status → PRE-CREATE
- Lọc giao dịch giá ≤ 0
- Chuẩn hoá cột ngày về date type
- Loại giao dịch trùng hoàn toàn (customer × article × date)

**Output:** Ba file parquet tại `s3a://datalake/silver/`

## 3.4. Bảy Candidate Generators — SparkSubmitOperator (song song)

Mỗi generator ghi ra `s3a://datalake/silver/candidates/<tên>.parquet` với schema chung:
```python
customer_id: string
article_id: string
score: double       # điểm Min-Max normalize [0, 1]
strategy: string    # nguồn sinh
```

| Task | Script | Top-N | Logic |
|---|---|---|---|
| `cand_repurchase` | `candidate_repurchase.py` | Top-15 | User mua lại sản phẩm đã từng mua (8 tuần gần nhất) |
| `cand_popularity` | `candidate_popularity.py` | Top-30 | Bestseller toàn hệ thống 7 ngày gần nhất — fallback cold-start |
| `cand_sibling` | `candidate_sibling.py` | Top-15 | Cùng product_code, khác màu/size (85% sản phẩm có biến thể) |
| `cand_als` | `candidate_als.py` | Top-40 | ALS implicit feedback — latent factor collaborative |
| `cand_itemcf` | `candidate_itemcf.py` | Top-20 | Item-based CF — cosine similarity trên co-occurrence |
| `cand_categorical` | `candidate_categorical.py` | Top-40 | Bestseller theo (index_group, garment_group, colour_group) |
| `export_cart_fpgrowth` | `candidate_fpgrowth.py` | Top-6 | FP-Growth item-to-item → sinh `cart_recommendations.json` |

**Riêng `export_cart_fpgrowth`:** Không qua union. Sinh `cart_recommendations.json` (item-to-item co-purchase) → upload lên MinIO gold. Sau đó được export vào MongoDB collection `cart_recommendations`.

**Application args chung:**
```python
application_args=["--run_date", "2020-09-22"]  # dataset H&M dừng ở ngày này
```

## 3.5. union_master — SparkSubmitOperator

**Mục đích:** Gộp 7 luồng candidate → loại trùng (customer × article) → giữ lại metadata nguồn (sources, max scores).

**Script:** `bigdata/apps/union_master.py`

**Sau union:** Trung bình mỗi user có ~97 ứng viên — đầu vào cho feature engineering.

## 3.6. feature_label — SparkSubmitOperator

**Mục đích:** Sinh 24 features + nhãn cho LightGBM.

**Script:** `bigdata/apps/feature_label.py`

**Thiết lập thời gian:**
- `HISTORY_DAYS = 42` (6 tuần) cho feature
- Train window: `[max_date - 8 tuần, max_date - 1 tuần]`
- Label window: `[max_date - 1 tuần, max_date]` — tuần cuối là ground truth

**Downsample negatives:** Tỷ lệ negatives/positives ~50:1 → downsample về 10:1 → `scale_pos_weight = 10` trong LightGBM.

**Output:** `s3a://datalake/gold/train_features.parquet`

## 3.7. train_lightgbm — BashOperator

**Mục đích:** Train LightGBM binary classifier.

**Chạy trên:** Airflow worker (Python thuần, không qua Spark).

**Command:**
```bash
cd /opt/spark-apps && python train_lightgbm.py
```

**Hyperparameters chính:**
```python
objective = "binary"
metric = "auc"
n_estimators = 400
learning_rate = 0.03
num_leaves = 63
max_depth = 8
scale_pos_weight = 10
min_child_samples = 100
subsample = 0.8
colsample_bytree = 0.8
```

**Lý do binary thay vì lambdarank:** Nhanh hơn 20%, output là probability (dễ debug), tương thích mọi phiên bản LightGBM.

**Output:** Model file → upload lên MinIO gold.

## 3.8. predict_lightgbm — BashOperator

**Command:**
```bash
cd /opt/spark-apps && python predict_lightgbm.py --run_date 2020-09-22
```

**Logic:**
1. Load model từ MinIO
2. Đọc test features (cùng cấu trúc train nhưng không có nhãn)
3. Predict score cho ~97 candidates mỗi user
4. Rank trong user → cắt Top-12
5. **Fallback:** User không có candidate → time-decayed popularity (τ = 14 ngày) thay vì raw bestseller

**Output:**
- `s3a://datalake/gold/predictions/top12_recommendations.parquet`
- `s3a://datalake/gold/predictions/global_top12.json`
- `s3a://datalake/gold/predictions/age_bestsellers.json`

## 3.9. export_to_mongo — BashOperator

**Command:**
```bash
cd /opt/spark-apps && \
CART_REC_PATH=s3a://datalake/gold/predictions/cart_recommendations.json \
python export_to_mongo.py --run_date 2020-09-22
```

**Script:** `bigdata/apps/export_to_mongo.py`

**Bốn collection được ghi:**

| Collection | Số doc | Logic |
|---|---|---|
| `user_recommendations` | ~10K (demo) | Bulk upsert Top-12 per user |
| `global_trending` | 1 | Top-12 toàn hệ thống |
| `age_bestsellers` | 6 | Top-12 theo nhóm tuổi |
| `cart_recommendations` | 28K | Item-to-item từ FPGrowth |

**Tối ưu:** `bulk_write(ordered=False)` — MongoDB parallel-apply 5000 ops/batch, nhanh hơn upsert tuần tự ~100 lần.

## 3.10. notify_done — PythonOperator

**Mục đích:** Log hoàn tất. Trong production → thay bằng Slack/email/webhook.

```python
print(f"Pipeline {ctx['ds']} hoàn tất — MongoDB hm_recsys đã cập nhật.")
```

---

# 4. Custom Airflow Image

## 4.1. Dockerfile

**File:** `bigdata/docker-file.airflow.demo`

**Base image:** `apache/airflow:2.10.4-python3.12`

**OS-level:**
```dockerfile
RUN apt-get install -y --no-install-recommends openjdk-17-jre-headless libgomp1
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

`openjdk-17` — cần cho `spark-submit` (Spark 3.5 hỗ trợ Java 17).
`libgomp1` — OpenMP runtime cho LightGBM train song song. **Không có thì LightGBM crash: "cannot open libgomp.so.1".**

**Python packages:**
```dockerfile
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    pyspark==3.5.6 \
    pymongo==4.6.1 \
    lightgbm==4.1.0 \
    pandas==2.1.4 \
    pyarrow==14.0.2 \
    numpy==1.26.3 \
    boto3==1.34.34 \
    psycopg2-binary==2.9.9
```

**Quan trọng:** `pyspark==3.5.6` phải khớp chính xác version của Spark cluster. PySpark serialize task qua Pickle — version mismatch → lỗi "Python in worker has different version".

## 4.2. Mount volumes

```yaml
airflow:
  volumes:
    - ./dags:/opt/airflow/dags
    - ./apps:/opt/spark-apps
    - ./logs:/opt/airflow/logs
    - ./data:/opt/airflow/data
```

- `dags/` — Python DAG files
- `apps/` — Spark scripts (extract, clean, candidate, feature, train, predict, export)
- `logs/` — Airflow task logs
- `data/` — Share data với host

---

# 5. Docker Compose Integration

## 5.1. Service dependencies

```
airflow
  ├── postgres           (metadata DB)         — depends_on: health check
  ├── oltp-postgres      (OLTP data source)    — depends_on: health check
  ├── mongodb            (serving DB)           — depends_on: started
  ├── spark-master       (compute)             — depends_on: started
  └── minio-init         (bucket created)      — depends_on: completed
```

Airflow chỉ start khi **tất cả** dependency healthy — không có race condition khi trigger DAG.

## 5.2. Health checks

```yaml
postgres:
  healthcheck:
    test: ["CMD", "pg_isready", "-U", "airflow"]
    interval: 10s
    timeout: 5s
    retries: 5

oltp-postgres:
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U hm"]
    interval: 10s
    timeout: 5s
    retries: 5
```

---

# 6. Quy trình vận hành

## 6.1. Chạy demo end-to-end

```bash
# 1. Khởi động stack
cd bigdata
docker compose up -d --build
# Đợi ~30s cho Airflow init DB

# 2. Mở Airflow UI
# http://localhost:8082  (admin/admin)

# 3. Bật DAG recsys_pipeline_v1 → Trigger

# 4. Theo dõi Graph View trong Airflow UI

# 5. Kiểm tra MongoDB
# http://localhost:8083  (admin/admin)
```

## 6.2. Monitoring

- **Airflow UI** (`localhost:8082`): Trạng thái DAG, task logs, retry count
- **Spark UI** (`localhost:8080`): Executor metrics, stage breakdown, shuffle data
- **MinIO Console** (`localhost:9001`): Datalake bronze/silver/gold files
- **Mongo Express** (`localhost:8083`): Xem collections, count documents, query

## 6.3. Xử lý lỗi thường gặp

| Lỗi | Nguyên nhân | Xử lý |
|---|---|---|
| `Python in worker has different version` | PySpark version không khớp Spark cluster | Build lại Airflow image đúng version |
| `cannot open libgomp.so.1` | Thiếu libgomp1 | Thêm vào Dockerfile |
| `oltp-postgres: connection refused` | OLTP chưa seed xong | Tăng `timeout` của `wait_oltp_ready` sensor |
| `No such file or directory: /opt/spark-apps/...` | Volume mount sai path | Kiểm tra docker-compose.yml mount |
| MongoDB upsert chậm | `ordered=True` mặc định | Đổi thành `ordered=False` |

---

# 7. Đặc điểm thiết kế đáng lưu ý

## 7.1. Idempotent throughout

Mọi task đều **re-run được** mà không corrupt state:
- Spark ghi đè file parquet cùng path
- MongoDB dùng `upsert` — chạy lần 2 không trùng lặp
- OLTP seed là `INSERT` có `ON CONFLICT DO NOTHING`

## 7.2. Tách biệt Spark job và Python thuần

- **Spark job** (SparkSubmitOperator): Xử lý dữ liệu lớn — extract, clean, candidate, feature, union
- **Python thuần** (BashOperator): Train/predict LightGBM, export MongoDB — không cần distributed compute

## 7.3. Env propagation

```
Host env vars
  → Airflow container env
  → spark.executorEnv.* (truyền xuống Spark executors)
  → SPARK_CONF / PYTHONPATH trong executor
```

Không hardcode credentials trong DAG — tất cả từ environment.

## 7.4. schedule_interval = None

Dataset H&M dừng ở 2020-09-22. DAG demo trigger thủ công. Production: đổi thành `schedule_interval="0 2 * * 1"` (2h sáng thứ 2 hàng tuần).

---

# 8. Hạn chế và hướng phát triển

**Hạn chế hiện tại:**
- `run_date` hardcode `2020-09-22` — chưa dùng Airflow templating (`{{ ds }}`)
- Không có Prometheus/Grafana monitoring
- Không có CI/CD cho DAG changes
- Chưa có retry policy riêng cho từng task nặng (train/predict có thể cần retry_delay > 5 phút)

**Hướng phát triển:**
1. Wire `{{ ds }}` vào `--run_date` argument — tự động chạy theo ngày thực
2. Thêm Prometheus scrape endpoint `/metrics` trên Airflow và backend
3. GitHub Actions test DAG syntax và Spark scripts với `chispa`
4. Tách `train_lightgbm` thành DAG riêng (weekly retrain) — không cần chạy cùng pipeline với candidate generation

---

*Tài liệu này mô tả hệ thống Airflow của dự án H&M Personalized Fashion Recommendations theo trạng thái code tại bản `demo-flow`.*
