#!/bin/bash
# 01  How HDFS stores a file: blocks and replicas.
# Run:  docker compose exec namenode bash /exercises/01_blocks_and_replicas.sh
set -e
echo "== Writing a 300 MB file. Block size is 128 MB, so expect 3 blocks (128 + 128 + 44)."
dd if=/dev/urandom of=/tmp/big.bin bs=1M count=300 status=none
hdfs dfs -mkdir -p /lab
hdfs dfs -put -f /tmp/big.bin /lab/big.bin
rm /tmp/big.bin

echo; echo "== Where each block and its 3 replicas live (fsck):"
hdfs fsck /lab/big.bin -files -blocks -locations | grep -E "^/lab|blk_|Total blocks|Average block replication"

echo; echo "== Space: 300 MB of data uses about 900 MB of raw disk with replication 3."
hdfs dfs -du -s -h /lab/big.bin
echo "(first figure: data size, second: raw disk used across replicas)"

cat <<'NOTE'

Talking points
- The NameNode holds only metadata (file -> blocks -> DataNodes), in memory.
  DataNodes hold the bytes. Clients read and write blocks directly from DataNodes.
- Replication 3: one replica local (or random), two on another rack. Survives a
  node failure and a rack failure.
- Large blocks (128 MB) keep NameNode metadata small and favour long sequential
  reads, which is what batch analytics does.
NOTE
