#!/usr/bin/env bash
# Throughput benchmark: how fast can this cluster write, sort and read data?
# It generates 300 MB of random 100-byte records, sorts them with MapReduce, then
# checks the output is in order. Industry name: TeraGen, TeraSort, TeraValidate.
# Run from the lab folder:  bash ops/benchmark_throughput.sh
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T edge bash -s <<'EOF'
set -euo pipefail
JAR=/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar
ROWS=3000000   # x 100 bytes = 300 MB, about 900 MB of raw disk with replication 3
hdfs dfs -rm -r -f -skipTrash /bench >/dev/null

step() { s=$(date +%s); yarn jar $JAR "$@" 2>&1 | grep -E "completed successfully" || true; echo $(( $(date +%s) - s )); }
echo "== 1. Write: generate 300 MB of random records"; w=$(step teragen -Dmapreduce.job.maps=4 $ROWS /bench/in | tail -1)
echo "== 2. Sort them";                                 t=$(step terasort -Dmapreduce.job.reduces=2 /bench/in /bench/out | tail -1)
echo "== 3. Read back and check the order";             v=$(step teravalidate /bench/out /bench/check | tail -1)
hdfs dfs -rm -r -f -skipTrash /bench >/dev/null

echo
echo "  write  ${w}s   -> $(( 300 / (w > 0 ? w : 1) )) MB/s"
echo "  sort   ${t}s   -> $(( 300 / (t > 0 ? t : 1) )) MB/s"
echo "  check  ${v}s"
EOF

cat <<'NOTE'

Using the result
- Measure on the cluster you have, then size for the target. If this cluster
  sorts X MB/s and a nightly batch must sort 10 TB in 4 hours (about 700 MB/s),
  you need roughly 700/X times the capacity, plus headroom.
- Before buying, find what limits it (disk, network or CPU) from the metrics
  during the run. Here all nodes share one host disk, so the figure shows that
  disk, not a real cluster.
NOTE
