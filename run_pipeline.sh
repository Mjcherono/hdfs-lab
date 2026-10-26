#!/usr/bin/env bash
# The whole web-logs pipeline, end to end:
#   1. HDFS       land the raw logs in the data lake
#   2. MapReduce  busiest hours and status codes, on YARN
#   3. Spark      the capacity report, on YARN
#   4. Prometheus the cluster's numbers afterwards
# Open Grafana first (Last 30 minutes, refresh 10s) to watch each stage.
# Run from the lab folder:  bash run_pipeline.sh
set -euo pipefail
cd "$(dirname "$0")"

stage() { echo; echo "######## $1  ($(date -u +%H:%M:%S) UTC)"; }

# A fresh cluster (after "docker compose down") needs its platform folders and
# Spark libraries in HDFS first.
if ! docker compose exec -T edge hdfs dfs -test -e /apps/spark/spark-libs-3.5.3.zip 2>/dev/null; then
  stage "0/4 Bootstrap a fresh cluster"; bash ops/bootstrap_cluster.sh
fi
stage "1/4 HDFS: ingest";              bash jobs/ingest_web_logs.sh
stage "2/4 MapReduce on YARN";         bash jobs/busiest_hours_mapreduce.sh
stage "3/4 Spark on YARN";             bash jobs/capacity_report_spark.sh
stage "4/4 Prometheus snapshot";       bash ops/metrics_snapshot.sh
