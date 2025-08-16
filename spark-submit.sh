#!/bin/bash
set -euo pipefail

# Optional: pass RUN_ID as an arg, default to v1 if not provided
RUN_ID=${1:-v1}

docker compose exec -e RUN_ID=v6 spark-master bash -lc "
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --conf spark.pyspark.python=/opt/bitnami/python/bin/python3 \
    --conf spark.pyspark.driver.python=/opt/bitnami/python/bin/python3 \
    --conf spark.executorEnv.PYSPARK_PYTHON=/opt/bitnami/python/bin/python3 \
    /opt/streaming/main.py
"
