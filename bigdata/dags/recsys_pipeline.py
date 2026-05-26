"""
Recsys ETL pipeline — chạy weekly.

Luồng:
  wait_oltp_ready
   → extract_oltp_to_minio   (Spark JDBC → MinIO bronze)
   → step1_cleaning          (bronze → silver)
   → [7 candidate jobs song song] → union_master
   → feature_label → train_lightgbm → predict_lightgbm
   → export_to_mongo → notify

Spark jobs nhận MinIO/JDBC config qua env vars + spark.jars.packages cho s3a/postgres.
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.python import PythonSensor

APPS = "/opt/spark-apps"
SPARK_CONN = "spark_default"

# Env truyền xuống Spark driver/executor — datalake + OLTP config
_PASS_ENV = [
    "DATALAKE_BASE", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
    "OLTP_JDBC_URL", "OLTP_JDBC_USER", "OLTP_JDBC_PASSWORD",
]
SPARK_ENV_VARS = {k: os.environ.get(k, "") for k in _PASS_ENV if os.environ.get(k)}
SPARK_ENV_VARS["PYTHONPATH"] = APPS

SPARK_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.5.4",
])

SPARK_EXECUTOR_CONF = {
    "spark.master": "spark://spark-master:7077",
    "spark.jars.packages": SPARK_PACKAGES,
    **{f"spark.executorEnv.{k}": v for k, v in SPARK_ENV_VARS.items()},
}

default_args = {
    "owner": "recsys",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "email_on_failure": False,
}


def check_oltp_ready() -> bool:
    """Sensor: ping OLTP Postgres và verify bảng transactions có data."""
    import psycopg2
    try:
        conn = psycopg2.connect(
            host="oltp-postgres", port=5432, dbname="hm_oltp",
            user=os.environ.get("OLTP_JDBC_USER", "hm"),
            password=os.environ.get("OLTP_JDBC_PASSWORD", "hm"),
        )
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM transactions")
        n = cur.fetchone()[0]
        conn.close()
        return n > 0
    except Exception as e:
        print(f"OLTP not ready: {e}")
        return False


with DAG(
    dag_id="recsys_pipeline_v1",
    description="ETL pipeline: candidates → ranking → MongoDB",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,         # trigger thủ công cho demo
    catchup=False,
    max_active_runs=1,
    tags=["recsys", "hm"],
) as dag:

    wait_oltp = PythonSensor(
        task_id="wait_oltp_ready",
        python_callable=check_oltp_ready,
        poke_interval=15,
        timeout=600,
        mode="reschedule",
    )

    extract_oltp = SparkSubmitOperator(
        task_id="extract_oltp_to_minio",
        application=f"{APPS}/extract_oltp_to_minio.py",
        conn_id=SPARK_CONN,
        conf=SPARK_EXECUTOR_CONF,
        env_vars=SPARK_ENV_VARS,
        py_files=f"{APPS}/common.py",
        verbose=True,
    )

    step1_clean = SparkSubmitOperator(
        task_id="step1_cleaning",
        application=f"{APPS}/step1_cleaning.py",
        conn_id=SPARK_CONN,
        conf=SPARK_EXECUTOR_CONF,
        env_vars=SPARK_ENV_VARS,
        py_files=f"{APPS}/common.py",
        verbose=True,
    )

    common_spark_args = dict(
        conn_id=SPARK_CONN,
        conf=SPARK_EXECUTOR_CONF,
        env_vars=SPARK_ENV_VARS,
        py_files=f"{APPS}/common.py",
        verbose=True,
        # Dataset H&M dừng ở 2020-09-22 — hardcode để demo có data
        application_args=["--run_date", "2020-09-22"],
    )

    cand_repurchase  = SparkSubmitOperator(task_id="cand_repurchase",
        application=f"{APPS}/candidate_repurchase.py", **common_spark_args)
    cand_popularity  = SparkSubmitOperator(task_id="cand_popularity",
        application=f"{APPS}/candidate_popularity.py", **common_spark_args)
    cand_sibling     = SparkSubmitOperator(task_id="cand_sibling",
        application=f"{APPS}/candidate_sibling.py", **common_spark_args)
    cand_als         = SparkSubmitOperator(task_id="cand_als",
        application=f"{APPS}/candidate_als.py", **common_spark_args)
    cand_itemcf      = SparkSubmitOperator(task_id="cand_itemcf",
        application=f"{APPS}/candidate_itemcf.py", **common_spark_args)
    cand_categorical = SparkSubmitOperator(task_id="cand_categorical",
        application=f"{APPS}/candidate_categorical.py", **common_spark_args)
    cand_fpgrowth    = SparkSubmitOperator(task_id="cand_fpgrowth",
        application=f"{APPS}/candidate_fpgrowth.py", **common_spark_args)

    candidates = [cand_repurchase, cand_popularity, cand_sibling,
                  cand_als, cand_itemcf, cand_categorical, cand_fpgrowth]

    union = SparkSubmitOperator(
        task_id="union_master",
        application=f"{APPS}/union_master.py", **common_spark_args,
    )
    feat_label = SparkSubmitOperator(
        task_id="feature_label",
        application=f"{APPS}/feature_label.py", **common_spark_args,
    )

    train_lgbm = BashOperator(
        task_id="train_lightgbm",
        bash_command=f"cd {APPS} && python train_lightgbm.py",
    )
    predict_lgbm = BashOperator(
        task_id="predict_lightgbm",
        bash_command=f"cd {APPS} && python predict_lightgbm.py --run_date 2020-09-22",
    )
    export_mongo = BashOperator(
        task_id="export_to_mongo",
        bash_command=f"cd {APPS} && python export_to_mongo.py --run_date 2020-09-22",
    )

    notify = PythonOperator(
        task_id="notify_done",
        python_callable=lambda **ctx: print(
            f"Pipeline {ctx['ds']} hoàn tất — MongoDB hm_recsys đã cập nhật."
        ),
    )

    wait_oltp >> extract_oltp >> step1_clean >> candidates
    for c in candidates:
        c >> union
    union >> feat_label >> train_lgbm >> predict_lgbm >> export_mongo >> notify
