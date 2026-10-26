#!/usr/bin/env bash
# Spark on YARN: the same job as busiest_hours_mapreduce.sh (requests per hour and
# per status code), for a like-for-like comparison.
# Run from the lab folder:  bash jobs/busiest_hours_spark.sh
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T spark /opt/spark/bin/spark-submit \
  --master yarn --deploy-mode client --name busiest-hours-spark \
  --num-executors 3 --executor-cores 1 --executor-memory 640m \
  --conf spark.yarn.am.memory=512m \
  --conf spark.driver.host=spark \
  --conf spark.sql.shuffle.partitions=12 \
  /opt/lab/jobs/busiest_hours_spark.py 2> >(grep -E "ERROR|Exception|Submitted application" >&2)

docker compose exec -T edge bash -s <<'EOF'
echo "== Ten busiest hours (requests, hour)"
hdfs dfs -cat /lake/marts/spark_busiest_hours/part-* | head -10 || true
echo; echo "== Status codes (requests, code)"
hdfs dfs -cat /lake/marts/spark_status_codes/part-* | head -10 || true
EOF
