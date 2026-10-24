#!/bin/bash
# 02  What happens when a DataNode dies: re-replication.
# Run ON THE HOST from the lab folder (it uses docker):  bash exercises/02_datanode_failure_HOST.sh
# Needs exercise 01's file in HDFS.
set -e
nn() { docker compose exec -T namenode "$@"; }
echo "== Before: replicas of /lab/big.bin"
nn hdfs fsck /lab/big.bin -files -blocks -locations | grep -E "blk_" | head -3
echo; echo "== Stopping datanode2"
docker compose stop datanode2
echo "Waiting for the NameNode to declare it dead."
echo "Dead after 2 x recheck interval (30 s here) + 10 x heartbeat (3 s) = about 90 s,"
echo "though it often shows sooner because the recheck runs on a timer."
# Three phases: the NameNode notices the node is dead (live=3), then counts its
# replicas as lost (replication drops below 3), then re-replicates (back to 3.0).
# Checking for "3.0" alone is not enough: it still reads 3.0 for a few seconds
# after the node is declared dead, before the lost replicas are counted.
seen_drop=0
for i in $(seq 1 72); do
  live=$(nn hdfs dfsadmin -report 2>/dev/null | grep -oP "Live datanodes \(\K[0-9]+")
  avg=$(nn hdfs fsck /lab/big.bin 2>/dev/null | grep -oP "Average block replication:\s+\K[0-9.]+")
  under=$(nn hdfs fsck / 2>/dev/null | grep -oP "Under-replicated blocks:\s+\K[0-9]+" || echo "?")
  [ "$avg" != "3.0" ] && seen_drop=1
  echo "  t+$((i*5))s  live=$live  under-replicated=$under  avg replication of big.bin=$avg"
  if [ "$live" = "3" ] && [ "$seen_drop" = "1" ] && [ "$avg" = "3.0" ] && [ "$under" = "0" ]; then done_ok=1; break; fi
  sleep 5
done
[ "${done_ok:-0}" = "1" ] || echo "  (still re-replicating after 6 minutes: check again with 'hdfs fsck /' before restarting datanode2)"
echo; echo "== After: every block back to 3 replicas, now on the 3 remaining DataNodes"
nn hdfs fsck /lab/big.bin | grep -E "Average block replication|Under-replicated"
echo; echo "== Restarting datanode2 (it rejoins; the NameNode removes the extra replicas)"
docker compose start datanode2
cat <<'NOTE'

Talking points
- Failure is normal at scale. HDFS detects it through missed heartbeats and
  re-replicates from the surviving copies, with no operator action.
- Capacity planning: keep enough free space on the remaining nodes to absorb a
  lost node's data. Losing 1 of N nodes needs about 1/N of total capacity free.
- Re-replication costs network and disk I/O, so it is throttled
  (dfs.namenode.replication.work.multiplier.per.iteration).
- It is throttled per BLOCK, not per byte. If exercise 03's small files are
  still in HDFS, watch the under-replicated count: about 1,400 tiny blocks drain
  at a few blocks a second while big.bin's two 128 MB blocks wait their turn.
  Small files slow recovery from failure, not only NameNode memory: another
  reason to keep files large.
NOTE
