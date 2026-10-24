#!/bin/bash
# 04  MapReduce on YARN: word count, the "hello world" of big data.
# Run:  docker compose exec resourcemanager bash /exercises/04_mapreduce_wordcount.sh
set -e
JAR=/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar
echo "== Building a corpus of 45 different documents (the licence texts shipped with Hadoop)"
mkdir -p /tmp/corpus
cp /opt/hadoop/licenses-binary/*.txt /opt/hadoop/LICENSE.txt /opt/hadoop/NOTICE.txt /opt/hadoop/README.txt /tmp/corpus/
hdfs dfs -rm -r -f -skipTrash /lab/corpus /lab/wordcount >/dev/null
hdfs dfs -put /tmp/corpus /lab/corpus && rm -rf /tmp/corpus
echo "== Running wordcount (watch it at http://localhost:18088)"
yarn jar $JAR wordcount /lab/corpus /lab/wordcount 2>&1 | grep -E "Running job|map 100% reduce 100%|completed successfully|Launched map tasks|Map input records|Map output records|Reduce input groups|Reduce output records|Bytes Written" || true
echo; echo "== Top 10 words"
hdfs dfs -cat /lab/wordcount/part-r-* | sort -k2 -nr | head -10
cat <<'NOTE'

Talking points
- Map: each mapper reads one input split (usually one HDFS block, run where the
  block lives: data locality) and emits (word, 1).
- Shuffle and sort: pairs are partitioned by key and sent to reducers. This is
  the expensive, network-heavy step.
- Reduce: sums the counts per word.
- A combiner (local pre-aggregation) cuts shuffle volume. wordcount uses one.
- Every stage writes to disk, which makes MapReduce robust but slow for
  multi-step or iterative jobs. That is what Spark improves on.
NOTE
