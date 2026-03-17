docker exec -it 985f32b51f8b6384a1e7d97208878a7478654a79fa59d211db984f38c9ba3cc2 /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/wordcount.py

docker exec -it 985f32b51f8b6384a1e7d97208878a7478654a79fa59d211db984f38c9ba3cc2 /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/test-read-file.py