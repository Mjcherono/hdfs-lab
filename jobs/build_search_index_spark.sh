#!/usr/bin/env bash
# Spark on YARN, cluster mode: build the inverted index (build_search_index_spark.py),
# then fetch the serving copy for tools/search.py.
# Run from the lab folder:  bash jobs/build_search_index_spark.sh
# Sizing as in capacity_report_spark.sh: driver and 3 executors, 4,096 MB in all.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T spark /opt/spark/bin/spark-submit \
  --name build-search-index \
  --driver-memory 512m --conf spark.driver.memoryOverhead=384m \
  --num-executors 3 --executor-cores 1 --executor-memory 640m \
  --conf spark.executor.memoryOverhead=384m \
  --conf spark.sql.shuffle.partitions=12 \
  /opt/lab/jobs/build_search_index_spark.py 2>&1 \
  | grep -E "Submitted application|final status|Exception|ERROR" || true

docker compose exec -T edge hdfs dfs -cat /lake/reports/search_index/part-*

# The serving copy goes to the search server's local disk, as a deploy step would.
mkdir -p data/search
docker compose exec -T edge hdfs dfs -cat '/lake/search/serving/postings/part-*' > data/search/postings.tsv
docker compose exec -T edge hdfs dfs -cat '/lake/search/serving/docs/part-*' > data/search/docs.tsv
echo "Serving copy: $(wc -l < data/search/postings.tsv) terms, $(wc -l < data/search/docs.tsv) documents in data/search/"
