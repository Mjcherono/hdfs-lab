#!/usr/bin/env bash
# HDFS health and capacity: what HDFS reports, what it really has, and whether
# every block has its replicas.
# Run from the lab folder:  bash ops/hdfs_health.sh
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T edge bash -s <<'EOF'
echo "== Capacity as HDFS reports it"
hdfs dfsadmin -report 2>/dev/null | sed -n '1,/^-----/p' \
  | grep -E '^(Configured Capacity|DFS Used|DFS Remaining|Live datanodes|Dead datanodes)' | head -6
hdfs dfsadmin -report 2>/dev/null | grep -E '^(Live|Dead) datanodes'

echo; echo "== Block health"
hdfs fsck / 2>/dev/null | sed '/Erasure Coded Block Groups/q' | grep -E 'Total size|Total files|Total blocks|Average block replication|Under-replicated blocks|Missing blocks:|Corrupt blocks|Status'

echo; echo "== Space by lake zone (data size, then raw disk across replicas)"
hdfs dfs -du -h /lake 2>/dev/null || echo "(no /lake yet: run jobs/ingest_web_logs.sh)"
EOF

echo; echo "== Real free disk under the DataNodes"
docker compose exec -T datanode1 df -h /tmp | tail -1 | awk '{print "   " $4 " free of " $2 " (the Docker disk on the host)"}'

cat <<'NOTE'

Reading it
- HDFS adds up the disk each DataNode reports. Here all four DataNodes sit on
  the same host disk, so "Configured Capacity" counts it four times. Real free
  space is the last figure. On real clusters the same trap appears when
  DataNodes share a SAN or virtual disks on one array.
- Raw disk = data size x replication (3). Plan disk from the raw figure.
- Under-replicated above 0: a DataNode is down or just came back. Missing or
  corrupt above 0: data loss, act now.
NOTE
