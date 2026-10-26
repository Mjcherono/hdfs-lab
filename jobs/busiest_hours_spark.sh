#!/usr/bin/env bash
# Spark on YARN, cluster mode: the same job as busiest_hours_mapreduce.sh (requests
# per hour and per status code), for a like-for-like comparison.
# Run from the lab folder:  bash jobs/busiest_hours_spark.sh
# Sizing as in capacity_report_spark.sh: driver and 3 executors, 4,096 MB in all.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T spark /opt/spark/bin/spark-submit \
  --name busiest-hours-spark \
  --driver-memory 512m --conf spark.driver.memoryOverhead=384m \
  --num-executors 3 --executor-cores 1 --executor-memory 640m \
  --conf spark.executor.memoryOverhead=384m \
  --conf spark.sql.shuffle.partitions=12 \
  /opt/lab/jobs/busiest_hours_spark.py 2>&1 \
  | grep -E "Submitted application|final status|Exception|ERROR" || true

docker compose exec -T edge bash -s <<'EOF'
echo "== Ten busiest hours (requests, hour)"
hdfs dfs -cat /lake/marts/spark_busiest_hours/part-* | head -10 || true
echo; echo "== Status codes (requests, code)"
hdfs dfs -cat /lake/marts/spark_status_codes/part-* | head -10 || true
EOF
