#!/usr/bin/env bash
# MapReduce on YARN: the busiest hours and the status code mix, straight from the
# raw logs. Uses Hadoop's built-in "grep" job, which counts regex matches.
# Run from the lab folder:  bash jobs/busiest_hours_mapreduce.sh
# Watch it: YARN UI http://localhost:18088, Grafana "YARN" panels.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T edge bash -s <<'EOF'
set -euo pipefail
JAR=/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar
IN=/lake/raw/nasa_http
OPTS="-Dmapreduce.input.fileinputformat.input.dir.recursive=true"
hdfs dfs -rm -r -f -skipTrash /lake/marts/mr_busiest_hours /lake/marts/mr_status_codes >/dev/null

run() {   # name, output dir, regex: each run is two MapReduce jobs (count, then sort)
  echo "== $1"; s=$(date +%s)
  yarn jar $JAR grep $OPTS $IN "$2" "$3" 1 2>&1 \
    | grep -E "Submitted application|completed successfully|Launched map tasks|Launched reduce tasks|FILE: Number of bytes written" || true
  echo "   took $(( $(date +%s) - s )) s"
}
run "Requests per hour (key: day/month/year:hour)" /lake/marts/mr_busiest_hours '\[([0-9]{2}/[A-Za-z]{3}/[0-9]{4}:[0-9]{2})'
run "Requests per HTTP status code"                /lake/marts/mr_status_codes  '" ([0-9]{3}) '

echo; echo "== Ten busiest hours (requests, hour)"
hdfs dfs -cat /lake/marts/mr_busiest_hours/part-r-* | head -10 || true
echo; echo "== Status codes (requests, code)"
hdfs dfs -cat /lake/marts/mr_status_codes/part-r-* | head -10 || true
EOF

cat <<'NOTE'

How it ran
- Map: one task per input split. Each .gz file is one split (gzip cannot be
  split), so two map tasks read all 3.5 million lines. Each emits (key, 1)
  for every regex match, with a combiner adding up counts locally first.
- Shuffle and sort: the counts are grouped by key and sent to a reducer.
- Reduce: adds the counts per key. A second job sorts the result by count.
- Every stage writes to disk: robust, but slow for multi-step work. The Spark
  job does the full report in memory instead.
NOTE
