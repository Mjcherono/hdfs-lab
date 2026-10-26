#!/usr/bin/env bash
# One-time setup of a fresh cluster: the HDFS folders the platform needs, and
# Spark's libraries published to HDFS. Safe to run again.
# Run from the lab folder:  bash ops/bootstrap_cluster.sh
set -euo pipefail
cd "$(dirname "$0")/.."
LIBS=spark-libs-3.5.3.zip

echo "== Refreshing the Spark client's copy of the cluster config"
for f in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml; do
  docker compose cp -q edge:/opt/hadoop/etc/hadoop/$f hadoop-conf/$f 2>/dev/null \
    || docker compose cp edge:/opt/hadoop/etc/hadoop/$f hadoop-conf/$f >/dev/null
done

echo "== Packing Spark's libraries ($(docker compose exec -T spark sh -c 'ls /opt/spark/jars | wc -l') jars)"
docker compose exec -T spark python3 - <<EOF
import glob, os, zipfile
with zipfile.ZipFile("/tmp/$LIBS", "w", zipfile.ZIP_STORED) as z:
    for jar in sorted(glob.glob("/opt/spark/jars/*.jar")):
        z.write(jar, os.path.basename(jar))
EOF
docker compose cp -q spark:/tmp/$LIBS data/$LIBS 2>/dev/null || docker compose cp spark:/tmp/$LIBS data/$LIBS >/dev/null

echo "== Creating platform folders in HDFS and publishing the libraries"
docker compose exec -T edge bash -s <<EOF
set -e
hdfs dfs -mkdir -p /apps/spark /spark-history /app-logs /lake/raw /lake/curated /lake/marts /lake/reports
hdfs dfs -chmod 1777 /spark-history /app-logs
hdfs dfs -put -f /input/$LIBS /apps/spark/$LIBS
hdfs dfs -ls /apps/spark
EOF
rm -f data/$LIBS
docker compose exec -T spark rm -f /tmp/$LIBS
echo "Done. Spark jobs now fetch their libraries from HDFS."
