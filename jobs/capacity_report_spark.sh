#!/usr/bin/env bash
# Spark on YARN, cluster mode: the capacity report (capacity_report_spark.py).
# Run from the lab folder:  bash jobs/capacity_report_spark.sh
# Cluster-wide settings (YARN, cluster mode, libraries, event logs) live in
# spark-conf/spark-defaults.conf. This script holds only this job's sizing:
#   driver     512 MB + 384 MB overhead = 896 MB, rounded up by YARN to 1,024 MB
#   executors  3 x (640 MB + 384 MB overhead) = 3 x 1,024 MB, 1 core each
#   total      4,096 MB: exactly the two NodeManagers' 2 x 2,048 MB
#   shuffle    12 partitions, about 4 per executor core
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T spark /opt/spark/bin/spark-submit \
  --name capacity-report \
  --driver-memory 512m --conf spark.driver.memoryOverhead=384m \
  --num-executors 3 --executor-cores 1 --executor-memory 640m \
  --conf spark.executor.memoryOverhead=384m \
  --conf spark.sql.shuffle.partitions=12 \
  /opt/lab/jobs/capacity_report_spark.py 2>&1 \
  | grep -E "Submitted application|final status|Exception|ERROR" || true

# The driver ran inside the cluster, so its report is read back from HDFS.
docker compose exec -T edge hdfs dfs -cat /lake/reports/capacity_report/part-*
