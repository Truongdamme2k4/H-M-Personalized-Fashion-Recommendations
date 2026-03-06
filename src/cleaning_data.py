import os
import sys
import shutil
import csv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col

# Fix lỗi hiển thị
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

def main():
    # --- 1. CẤU HÌNH ---
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    
    # Folder temp để tránh lỗi Windows xóa file
    spark_temp_dir = os.path.join(project_root, "spark_temp")
    if not os.path.exists(spark_temp_dir):
        os.makedirs(spark_temp_dir)

    print(f"Project Root: {project_root}")

    # --- 2. INIT SPARK ---
    spark = SparkSession.builder \
        .appName("Instacart_Cleaning_M2") \
        .config("spark.driver.memory", "4g") \
        .config("spark.local.dir", spark_temp_dir) \
        .getOrCreate()
    
    # --- 3. SCHEMA ---
    schema_orders = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("eval_set", StringType(), True),
        StructField("order_number", IntegerType(), True),
        StructField("order_dow", IntegerType(), True),
        StructField("order_hour_of_day", IntegerType(), True),
        StructField("days_since_prior_order", DoubleType(), True)
    ])
    
    schema_products = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("aisle_id", IntegerType(), True),
        StructField("department_id", IntegerType(), True)
    ])
    
    schema_order_products = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("add_to_cart_order", IntegerType(), True),
        StructField("reordered", IntegerType(), True)
    ])

    # --- 4. LOAD DATA ---
    print("Đang đọc dữ liệu...")
    try:
        df_orders = spark.read.csv(os.path.join(project_root, "data/raw/orders.csv"), header=True, schema=schema_orders)
        df_products = spark.read.csv(os.path.join(project_root, "data/raw/products.csv"), header=True, schema=schema_products)
        df_op_prior = spark.read.csv(os.path.join(project_root, "data/raw/order_products__prior.csv"), header=True, schema=schema_order_products)
        df_op_train = spark.read.csv(os.path.join(project_root, "data/raw/order_products__train.csv"), header=True, schema=schema_order_products)
    except Exception as e:
        print(f"LỖI LOAD FILE: {e}")
        return

    df_op = df_op_prior.union(df_op_train)

    # --- 5. CLEANING ---
    print("Đang xử lý làm sạch...")
    df_orders_clean = df_orders.na.fill({"days_since_prior_order": 0.0})
    
    order_counts = df_op.groupBy("order_id").count().withColumnRenamed("count", "num_items")
    valid_orders = order_counts.filter(col("num_items") >= 2).select("order_id")
    df_op_clean = df_op.join(valid_orders, "order_id", "inner")
    
    final_df = df_op_clean.join(df_orders_clean, "order_id", "inner") \
                          .join(df_products, "product_id", "inner") \
                          .select("order_id", "user_id", "product_name", "order_dow", "order_hour_of_day")

    # --- 6. XUẤT FILE BẰNG PYTHON ---
    output_csv = os.path.join(project_root, "data/processed/instacart_cleaned.csv")
    print(f"Đang lưu file tại: {output_csv}")
    
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(final_df.columns)
        
        # Dùng toLocalIterator để lấy từng dòng về
        count = 0
        for row in final_df.toLocalIterator():
            writer.writerow(row)
            count += 1
            if count % 100000 == 0:
                print(f">> Đã ghi được {count} dòng...")

    print("Hoàn thành!")
    print(f"File kết quả: {output_csv}")
    
    spark.stop()
    # Xóa temp
    if os.path.exists(spark_temp_dir):
        shutil.rmtree(spark_temp_dir, ignore_errors=True)

if __name__ == "__main__":
    main()