"""
Recsys ETL pipeline — chạy weekly.

Luồng:
  wait_raw_data
   → step1_cleaning
   → [6 candidate jobs song song] → union_master
   → feature_label → train_lightgbm → predict_lightgbm
   → export_to_mongo → notify

Scripts Spark dùng SparkSubmitOperator (conn 'spark_default').
Train/predict/export dùng BashOperator (chạy thuần Python trong container airflow).

Mọi script đọc env HDFS_BASE để biết nên đọc HDFS hay local FS.
Trong demo compose: HDFS_BASE=file:///workspace, data ở /workspace/data/raw/.
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
SPARK_CONF = {"spark.master": "spark://spark-master:7077"}

# Env truyền xuống executor để các script biết base path (HDFS vs file://)
HDFS_BASE = os.environ.get("HDFS_BASE", "hdfs://namenode:9000")
SPARK_ENV_VARS = {
    "HDFS_BASE": HDFS_BASE,
    "PYTHONPATH": APPS,
}
SPARK_EXECUTOR_CONF = {
    **SPARK_CONF,
    "spark.executorEnv.HDFS_BASE": HDFS_BASE,
    "spark.executorEnv.PYTHONPATH": APPS,
}

default_args = {
    "owner": "recsys",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "email_on_failure": False,
}


def check_raw_data() -> bool:
    """Sensor: kiểm tra transactions_train.csv tồn tại (HDFS hoặc local)."""
    if HDFS_BASE.startswith("file://"):
        path = HDFS_BASE[len("file://"):] + "/data/raw/transactions_train.csv"
        return os.path.exists(path)
    import subprocess
    result = subprocess.run(
        ["hdfs", "dfs", "-test", "-e", "/data/raw/transactions_train.csv"],
        capture_output=True,
    )
    return result.returncode == 0


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

    wait_raw = PythonSensor(
        task_id="wait_raw_data",
        python_callable=check_raw_data,
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    step1_clean = SparkSubmitOperator(
        task_id="step1_cleaning",
        application=f"{APPS}/step1_cleaning.py",
        conn_id=SPARK_CONN,
        conf=SPARK_EXECUTOR_CONF,
        env_vars=SPARK_ENV_VARS,
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

    candidates = [cand_repurchase, cand_popularity, cand_sibling,
                  cand_als, cand_itemcf, cand_categorical]

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

    wait_raw >> step1_clean >> candidates
    for c in candidates:
        c >> union
    union >> feat_label >> train_lgbm >> predict_lgbm >> export_mongo >> notify
