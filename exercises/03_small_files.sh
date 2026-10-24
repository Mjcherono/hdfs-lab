#!/bin/bash
# 03  The small-files problem: why NameNode memory, not disk, can be the limit.
# Run:  docker compose exec namenode bash /exercises/03_small_files.sh
set -e
jmx() { curl -s "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem" | tr -d '\n' | grep -oP "\"$1\"\s*:\s*\K[0-9]+"; }
f0=$(jmx FilesTotal); b0=$(jmx BlocksTotal)
echo "== Before: files=$f0 blocks=$b0"

echo -e "== Writing 2,000 files of 1 KB each (2 MB of data in total). Takes 2 to 3 minutes:\n   every file is its own write pipeline, which is part of the lesson"
mkdir -p /tmp/small && for i in $(seq 1 2000); do head -c 1024 /dev/urandom > /tmp/small/f$i; done
hdfs dfs -put -f /tmp/small /lab/small
rm -rf /tmp/small
f1=$(jmx FilesTotal); b1=$(jmx BlocksTotal)
echo "== After:  files=$f1 blocks=$b1"
echo "== Added $((f1-f0)) files and $((b1-b0)) blocks: about $(( (f1-f0+b1-b0)*150/1024 )) KB of NameNode heap for 2 MB of data."
echo "   (Heap is estimated at about 150 bytes per object. At this size the real JVM"
echo "   heap figure is lost in garbage-collection noise, so we calculate it instead.)"

cat <<'NOTE'

What you just saw: 2 MB of data created about 2,000 files and 2,000 blocks.
The 300 MB file from exercise 01 created 1 file and 3 blocks.

Rule of thumb: every file, directory and block is an object of roughly 150 bytes
in NameNode heap, so about 1 GB of heap per million blocks.
  100 million 1 KB files = about 200 million objects = about 30 GB of heap for 100 GB of data.
  The same 100 GB as 128 MB files = about 800 files and blocks = a few hundred KB.

Fixes: combine small files (SequenceFile, Parquet, ORC, Hadoop archives),
compact on ingest, or partition by larger time windows.
NOTE
