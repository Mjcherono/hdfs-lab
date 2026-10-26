#!/usr/bin/env bash
# Spark on YARN: the capacity report from the web logs (capacity_report_spark.py).
# Run from the lab folder:  bash jobs/capacity_report_spark.sh
# Sizing: 3 executors x 1 core x 640 MB, plus a 512 MB application master, fits the
# two NodeManagers (2 GB each). Shuffle partitions: 12, about 4 per core.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T spark /opt/spark/bin/spark-submit \
  --master yarn --deploy-mode client --name capacity-report \
  --num-executors 3 --executor-cores 1 --executor-memory 640m \
  --conf spark.yarn.am.memory=512m \
  --conf spark.driver.host=spark \
  --conf spark.sql.shuffle.partitions=12 \
  --conf spark.sql.session.timeZone=UTC \
  /opt/lab/jobs/capacity_report_spark.py 2> >(grep -E "ERROR|Exception|Submitted application" >&2)
