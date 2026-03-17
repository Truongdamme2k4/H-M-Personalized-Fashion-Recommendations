# Cho container ZeroTier tham gia vào mạng ZeroTier để các máy có thể kết nối qua VPN
docker exec -it zerotier-one zerotier-cli join 154a350c86b1c5c7

# Khởi tạo Docker Swarm và quảng bá địa chỉ IP để các node khác join vào
docker swarm init --advertise-addr 10.229.91.65

# Tạo overlay network để các container trên nhiều node khác nhau có thể giao tiếp
docker network create --driver overlay --attachable bigdata_network

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


docker exec -it a9e8df6f6b6cb48477f0d5e74e57c8ac30aa92b9a49e126e04f7564c659bf32a hdfs dfs -mkdir -p /input
docker cp ./part-00000-f9d2db6d-69c2-4b6b-93f9-7ed91b828f70-c000.csv a9e8df6f6b6cb48477f0d5e74e57c8ac30aa92b9a49e126e04f7564c659bf32a:/tmp/file.csv
docker exec -it a9e8df6f6b6cb48477f0d5e74e57c8ac30aa92b9a49e126e04f7564c659bf32a hdfs dfs -put /tmp/file.csv /input/

docker exec -it a9e8df6f6b6cb48477f0d5e74e57c8ac30aa92b9a49e126e04f7564c659bf32a hdfs dfs -get /output/wordcount /tmp/wordcount
docker cp a9e8df6f6b6cb48477f0d5e74e57c8ac30aa92b9a49e126e04f7564c659bf32a:/tmp/wordcount D:\wordcount