#!/bin/bash
# 05  TeraGen and TeraSort: the classic cluster benchmark, scaled down.
# Run:  docker compose exec resourcemanager bash /exercises/05_terasort.sh
# 3,000,000 rows x 100 bytes = 300 MB (900 MB raw with replication 3), twice.
set -e
JAR=/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar
hdfs dfs -rm -r -f -skipTrash /lab/tera-in /lab/tera-out >/dev/null
echo "== TeraGen 300 MB"; s=$(date +%s)
yarn jar $JAR teragen -Dmapreduce.job.maps=4 3000000 /lab/tera-in 2>&1 | grep -E "completed successfully" || true
g=$(( $(date +%s) - s ))
echo "== TeraSort"; s=$(date +%s)
yarn jar $JAR terasort -Dmapreduce.job.reduces=2 /lab/tera-in /lab/tera-out 2>&1 | grep -E "completed successfully" || true
t=$(( $(date +%s) - s ))
echo "== Validate"; yarn jar $JAR teravalidate /lab/tera-out /lab/tera-val 2>&1 | grep -E "completed successfully" || true
echo; echo "Generate: ${g}s   Sort: ${t}s   for 300 MB  ->  about $(( 300 / (t>0?t:1) )) MB/s sort throughput"
hdfs dfs -rm -r -f -skipTrash /lab/tera-in /lab/tera-out /lab/tera-val >/dev/null
cat <<'NOTE'

Capacity planning use: measure throughput on a small cluster, then size for the
target. If a 4-node cluster sorts X MB/s and the nightly batch must sort 10 TB in
4 hours (about 700 MB/s), you need roughly 700/X times the current capacity,
plus headroom, then check the bottleneck (disk, network or CPU) before buying.
Here every node shares one physical disk, so the result shows that disk, not a
real cluster.
NOTE
