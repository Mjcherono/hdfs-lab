#!/usr/bin/env bash
# MapReduce vs Spark on the same job: requests per hour and per status code from
# the raw logs. Runs both, times them, counts the YARN applications and the
# intermediate data each wrote to local disk, and checks the answers are equal.
# Run from the lab folder:  bash jobs/compare_engines.sh
# Watch it: Grafana YARN panels show two separate bursts.
set -euo pipefail
cd "$(dirname "$0")/.."
edge() { docker compose exec -T edge "$@"; }
apps() { edge yarn application -list -appStates ALL 2>/dev/null | grep -c '^application_' || true; }

before=$(apps)
echo "######## MapReduce"
s=$(date +%s); bash jobs/busiest_hours_mapreduce.sh > /tmp/compare_mr.log 2>&1; mr_s=$(( $(date +%s) - s ))
mid=$(apps)
echo "   done in ${mr_s}s"

echo "######## Spark"
s=$(date +%s); bash jobs/busiest_hours_spark.sh > /tmp/compare_spark.log 2>&1; sp_s=$(( $(date +%s) - s ))
after=$(apps)
echo "   done in ${sp_s}s"

# MapReduce reports what each job spilled to local disk between map and reduce.
mr_disk=$(grep -oP 'FILE: Number of bytes written=\K[0-9]+' /tmp/compare_mr.log | awk '{s+=$1} END {print s+0}')

same() {  # sort both outputs the same way, then compare
  a=$(edge hdfs dfs -cat "/lake/marts/mr_$1/part-*" | sort); b=$(edge hdfs dfs -cat "/lake/marts/spark_$1/part-*" | sort)
  [ "$a" = "$b" ] && echo "identical ($(echo "$a" | wc -l) rows)" || echo "DIFFERENT"
}

cat <<EOF

                              MapReduce        Spark
  wall time                   $(printf '%-16s' "${mr_s} s") ${sp_s} s
  YARN applications           $(printf '%-16s' "$((mid - before))") $((after - mid))
  times the input was read    2                1 (cached in memory)
  intermediate data on disk   $(printf '%-16s' "$((mr_disk / 1000000)) MB") shuffle only, see Spark UI

  busiest hours result        $(same busiest_hours)
  status codes result         $(same status_codes)

How to read it
- MapReduce: each question is a fixed map-then-reduce job, plus a second job to
  sort. Four applications, each starting its own containers, reading the raw
  files again and writing every stage to disk.
- Spark: one application. It reads the files once, keeps them in memory and
  runs both questions as stages of the same plan.
- On 36 MB, start-up time dominates both. The gap widens with more steps and
  more data, which is why Spark replaced MapReduce for most work.
EOF
