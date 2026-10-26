#!/usr/bin/env bash
# 08a  Load the NASA web server logs into the raw zone of the data lake.
# Run on the edge node:  docker compose exec edge bash /exercises/08_load_web_logs.sh
# Expects the two files in ./data on the host (mounted at /input), see README.
set -euo pipefail

echo "== Raw zone: files kept exactly as delivered, one folder per month"
for f in /input/NASA_access_log_*95.gz; do
  mon=$(basename "$f" | sed -E 's/.*_(Jul|Aug)95\.gz/\1/')
  case $mon in Jul) m=07 ;; Aug) m=08 ;; esac
  hdfs dfs -mkdir -p /lake/raw/nasa_http/month=1995-$m
  hdfs dfs -put -f "$f" /lake/raw/nasa_http/month=1995-$m/
done
hdfs dfs -ls -R /lake/raw/nasa_http
echo; echo "== Blocks per file"
hdfs fsck /lake/raw/nasa_http -files -blocks 2>/dev/null | grep -E '\.gz .*block' || true

cat <<'NOTE'

Talking points
- Raw zone: keep the data exactly as it arrived, so any later result can be
  rebuilt from it. Cleaned and aggregated data go in separate zones.
- gzip is not splittable: Spark can only read each .gz file with ONE task, however
  many cores the cluster has. Two files = at most two readers. For big inputs,
  land data as splittable formats (Parquet, ORC, bzip2) or many smaller files.
- Partition folders (month=1995-07) let a job read only the months it needs.
NOTE
