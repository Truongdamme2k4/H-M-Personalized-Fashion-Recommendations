from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Định nghĩa các tham số mặc định
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_mongo_data():
    print("Đang kiểm tra kết nối tới MongoDB tại mongodb:27017...")
    # Bạn có thể dùng pymongo ở đây để check data thực tế
    return "Dữ liệu đã sẵn sàng!"

with DAG(
    'he_thong_big_data_v1',
    default_args=default_args,
    description='Luồng xử lý từ Mongo sang Spark',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    # Task 1: Kiểm tra nguồn dữ liệu
    task_check_data = PythonOperator(
        task_id='kiem_tra_nguon_data',
        python_callable=check_mongo_data
    )

    # Task 2: Chạy Job Spark (Giả sử bạn đã có file xử lý trong ./apps)
    # File này sẽ đọc từ Mongo và ghi vào HDFS
    task_run_spark = SparkSubmitOperator(
        task_id='chay_spark_process',
        application='/opt/spark-apps/wordcount.py', # Đường dẫn trong container spark
        conn_id='spark_default',
        conf={'spark.master': 'spark://spark-master:7077'},
        verbose=True
    )

    # Task 3: Thông báo hoàn tất
    task_finish = PythonOperator(
        task_id='hoan_tat',
        python_callable=lambda: print("Pipeline đã chạy xong mượt mà!")
    )

    # Thiết lập thứ tự chạy
    task_check_data >> task_run_spark >> task_finish