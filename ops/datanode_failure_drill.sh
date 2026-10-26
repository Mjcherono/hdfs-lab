#!/usr/bin/env bash
# Failure drill: stop one DataNode and watch HDFS restore 3 replicas of every
# block on the remaining nodes, with no operator action. Then bring it back.
# Run from the lab folder:  bash ops/datanode_failure_drill.sh
# Watch it: Grafana "Live DataNodes" and "Blocks needing attention".
set -euo pipefail
cd "$(dirname "$0")/.."
edge() { docker compose exec -T edge "$@"; }

blocks=$(edge hdfs fsck / 2>/dev/null | grep -oP 'Total blocks \(validated\):\s+\K[0-9]+' || echo 0)
[ "$blocks" -gt 0 ] || { echo "HDFS is empty: run jobs/ingest_web_logs.sh first."; exit 1; }

echo "== Before"
edge hdfs fsck / 2>/dev/null | grep -E 'Total blocks|Average block replication|Under-replicated blocks'

echo; echo "== Stopping datanode2"
docker compose stop datanode2 >/dev/null
echo "The NameNode declares it dead after missing heartbeats: here about 90 s"
echo "(2 x 30 s recheck + 10 x 3 s heartbeat). Then it copies the lost replicas."

# Three phases: node declared dead (live=3), its replicas counted as lost
# (under-replicated > 0), then re-replicated (back to 0).
seen=0; t0=$(date +%s)
for i in $(seq 1 240); do
  live=$(edge hdfs dfsadmin -report 2>/dev/null | grep -oP 'Live datanodes \(\K[0-9]+')
  under=$(edge hdfs fsck / 2>/dev/null | grep -oP 'Under-replicated blocks:\s+\K[0-9]+' || echo "?")
  [ "$under" != "0" ] && seen=1
  echo "  t+$(( $(date +%s) - t0 ))s  live DataNodes=$live  under-replicated blocks=$under"
  [ "$live" = "3" ] && [ "$seen" = 1 ] && [ "$under" = "0" ] && { ok=1; break; }
  sleep 5
done
if [ "${ok:-0}" = 1 ]; then
  echo; echo "== After $(( $(date +%s) - t0 )) s: every block back to 3 replicas, on the 3 remaining DataNodes"
else
  echo; echo "== Still repairing after $(( $(date +%s) - t0 )) s (state below); restarting datanode2 anyway"
fi
edge hdfs fsck / 2>/dev/null | grep -E 'Average block replication|Under-replicated blocks|Status'

echo; echo "== Restarting datanode2 (it rejoins; the NameNode removes the extra replicas)"
docker compose start datanode2 >/dev/null

cat <<'NOTE'

Capacity points
- Failure is normal at scale; HDFS repairs itself from the surviving copies.
- Keep enough free space to absorb a lost node: losing 1 of N nodes needs about
  1/N of total capacity free on the others.
- Repair costs network and disk I/O, so it is throttled per block. Many small
  files mean many blocks, and slower recovery.
NOTE
