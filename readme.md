# Cho container ZeroTier tham gia vào mạng ZeroTier để các máy có thể kết nối qua VPN
docker exec -it zerotier-one zerotier-cli join 154a350c86b1c5c7

# Khởi tạo Docker Swarm và quảng bá địa chỉ IP để các node khác join vào
docker swarm init --advertise-addr 10.229.91.65

# Tạo overlay network để các container trên nhiều node khác nhau có thể giao tiếp
docker network create --driver overlay --opt com.docker.network.driver.mtu=1500 --attachable bigdata_network

# Worker node tham gia vào cluster Docker Swarm bằng token
   docker swarm join --token SWMTKN-1-02hcs2ol7ls24z94b4l088yi32pqomek73em3j7azi344b75ap-d5xt8glyhwr8l1o9cu4lryykh 10.229.91.65:2377

# Triển khai toàn bộ hệ thống Hadoop + Spark bằng file docker-stack.yml
# Docker Swarm sẽ tự tạo các service như namenode, datanode, spark master, spark worker
docker stack deploy -c docker-stack.yml hadoop_cluster

# Xóa toàn bộ stack Hadoop cluster khi không cần sử dụng nữa
docker stack rm hadoop_cluster


docker pull bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
docker pull bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
docker pull bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8
docker pull bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8
docker pull bde2020/spark-master:3.1.1-hadoop3.2
docker pull bde2020/spark-worker:3.1.1-hadoop3.2
docker pull bde2020/spark-history-server:3.1.1-hadoop3.2

docker build -t jupyter-spark:3.1.1 -f docker-file.jupyter .
docker build -t airflow-spark:2.6.3 -f docker-file.airflow .

# hadoop resource
http://10.229.91.65:9870/

## hdfs ui
http://10.229.91.65:8888

# jupyter
http://10.229.91.65:8889



docker cp "C:\Users\TUAN ANH\Downloads\transactions_train.csv" 27af52f3964a5e63383ebc3f7dbe6c2bedcdf434bc27eea03329d9fe51d85458:/tmp/transactions_train.csv

docker exec -it 27af52f3964a5e63383ebc3f7dbe6c2bedcdf434bc27eea03329d9fe51d85458 bash

hdfs dfs -put /tmp/transactions_train.csv /data/raw/

hdfs dfs -ls /data/raw/


#!/bin/bash

# 1. Khai báo biến
SOURCE_FILE="/tmp/transactions_train.csv"
# Thêm /transactions vào cuối đường dẫn
DEST_DIR="/data/raw/transactions"
PART_PREFIX="/tmp/trans_part_"
BLOCK_SIZE="100M"

echo "--- BẮT ĐẦU QUY TRÌNH ---"

# 2. Tạo thư mục đích trên HDFS nếu chưa có
# Lệnh -p giúp tạo cả thư mục cha nếu chưa tồn tại
hdfs dfs -mkdir -p $DEST_DIR

# 3. Thoát Safe Mode (đề phòng trường hợp Hadoop vừa khởi động)
hdfs dfsadmin -safemode leave

# 4. Xóa các file cũ trong thư mục transactions để tránh xung đột
hdfs dfs -rm -r $DEST_DIR/trans_part_* 2>/dev/null
rm -f ${PART_PREFIX}*

# 5. Chia nhỏ file
echo "Đang chia nhỏ file $SOURCE_FILE thành các phần $BLOCK_SIZE..."
split -b $BLOCK_SIZE $SOURCE_FILE $PART_PREFIX -d -a 2

# 6. Vòng lặp đẩy từng file lên HDFS
for file in ${PART_PREFIX}*; do
    filename=$(basename $file)
    echo "---------------------------------------"
    echo "Đang đẩy $filename lên $DEST_DIR..."
    
    # Tham số tối ưu cho mạng ZeroTier
    hdfs dfs -D dfs.client.socket-timeout=180000 \
             -D dfs.client.write.bandwidthPerSec=5242880 \
             -put "$file" "$DEST_DIR/"
    
    if [ $? -eq 0 ]; then
        echo "=> THÀNH CÔNG: $filename"
        rm -f "$file"
    else
        echo "=> THẤT BẠI: $filename. Dừng quy trình!"
        exit 1
    fi
done

echo "--- HOÀN THÀNH ---"
hdfs dfs -ls $DEST_DIR


---

# Chạy Demo App (Backend + Frontend)

Ứng dụng demo gợi ý sản phẩm gồm 2 service: `backend` (Node.js/Express) và `frontend` (React + Vite).

## Yêu cầu
- Node.js >= 18
- Các file dữ liệu ở thư mục gốc repo: `article_metadata.csv`, `final_recommendations.csv`, `cart_recommendations.json`, `similar_products.json`, `global_trending.json`

## Cài đặt dependencies (chạy 1 lần)
```bash
cd backend && npm install
cd ../frontend && npm install
```

## Cấu hình backend
File `backend/.env` đã có sẵn. Các biến chính:
```
PORT=4000
DATA_DIR=../
CORS_ORIGIN=http://localhost:5173
IMAGE_TEMPLATE=         # để trống → dùng placeholder theo category (placehold.co)
```

## Chạy backend (Terminal 1)
```bash
cd backend
npm run dev
```
Backend chạy tại `http://localhost:4000`. Health check: `http://localhost:4000/health`.

## Chạy frontend (Terminal 2)
```bash
cd frontend
npm run dev
```
Mở `http://localhost:5173` trong trình duyệt.

## Các tính năng
- **Feature 1 · Homepage**: nhập `customer_id` → hiển thị 12 sản phẩm cá nhân hoá (có fallback `global_trending` nếu không tồn tại).
- **Feature 2 · Cart · Cross-sell**: thêm sản phẩm vào giỏ → gợi ý 6 món "thường mua cùng".
- **Feature 3 · Up-sell · Similar**: nhập `article_id` (hoặc click một sản phẩm) → hiển thị 6 sản phẩm tương tự.

## Dừng services
- Ctrl+C ở cả 2 terminal.
- Nếu port 4000 bị giữ: `netstat -ano | grep :4000` rồi `taskkill //F //PID <pid>`.
