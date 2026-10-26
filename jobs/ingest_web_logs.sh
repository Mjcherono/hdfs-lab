#!/usr/bin/env bash
# Land the NASA web server logs in the raw zone of the data lake (HDFS).
# Run from the lab folder:  bash jobs/ingest_web_logs.sh
# Expects the two .gz files in ./data (see README); the edge node sees them as /input.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T edge bash -s <<'EOF'
set -euo pipefail
echo "== Raw zone: files kept exactly as delivered, one folder per month"
for f in /input/NASA_access_log_*95.gz; do
  case $(basename "$f") in *Jul95*) m=07 ;; *Aug95*) m=08 ;; *) continue ;; esac
  hdfs dfs -mkdir -p /lake/raw/nasa_http/month=1995-$m
  hdfs dfs -put -f "$f" /lake/raw/nasa_http/month=1995-$m/
done
hdfs dfs -ls -R /lake/raw/nasa_http
echo; echo "== Blocks and replicas per file"
hdfs fsck /lake/raw/nasa_http -files -blocks 2>/dev/null | grep -E '\.gz .*block' || true
EOF

cat <<'NOTE'

Notes
- Raw zone: data exactly as it arrived, so every later result can be rebuilt.
  Cleaned data and aggregates go in separate zones (curated, marts).
- Each file is under 128 MB, so it is one block, stored 3 times (replication 3).
- gzip cannot be split: each file is read by ONE task however big the cluster.
  For large inputs, land splittable formats (Parquet, ORC) or many files.
- month= folders are partitions: a job reads only the months it needs.
NOTE
