#!/usr/bin/env bash
# The cluster's key numbers right now, read from Prometheus, which scrapes every
# Hadoop daemon each 15 s. The same queries drive the Grafana dashboard.
# Run from the lab folder:  bash ops/metrics_snapshot.sh
set -euo pipefail

python3 - <<'EOF'
import json, urllib.parse, urllib.request

PROM = "http://127.0.0.1:19090/api/v1/query?"
def q(expr):
    try:
        r = json.load(urllib.request.urlopen(PROM + urllib.parse.urlencode({"query": expr}), timeout=5))
    except OSError as exc:
        raise SystemExit(f"Prometheus not reachable on 127.0.0.1:19090: {exc}")
    res = r["data"]["result"]
    return float(res[0]["value"][1]) if res else None

rows = [
    ("Scrape targets up",              'sum(up)', "{:.0f} of " + str(int(q('count(up)') or 0))),
    ("Live DataNodes",                 'fs_namesystem_num_live_data_nodes', "{:.0f}"),
    ("HDFS used (raw, all replicas)",  'fs_namesystem_capacity_used / 1e9', "{:.2f} GB"),
    ("Files / blocks",                 'fs_namesystem_files_total', "{:.0f}"),
    ("",                               'fs_namesystem_blocks_total', "{:.0f} blocks"),
    ("Under-replicated blocks",        'fs_namesystem_low_redundancy_blocks', "{:.0f}"),
    ("YARN memory in use",             'queue_metrics_allocated_mb{queue="root"}', "{:.0f} MB"),
    ("YARN memory free",               'queue_metrics_available_mb{queue="root"}', "{:.0f} MB"),
    ("Containers running / waiting",   'queue_metrics_allocated_containers{queue="root"}', "{:.0f} running"),
    ("",                               'queue_metrics_pending_containers{queue="root"}', "{:.0f} waiting"),
    ("Jobs run since YARN started",    'queue_metrics_apps_completed{queue="root"}', "{:.0f}"),
    ("Peak YARN memory, last hour",    'max_over_time(queue_metrics_allocated_mb{queue="root"}[1h])', "{:.0f} MB"),
]
print("== Cluster now (from Prometheus)")
for label, expr, fmt in rows:
    v = q(expr)
    print(f"  {label:32} {fmt.format(v) if v is not None else 'no data'}")
print("\nDashboard: Grafana http://localhost:13000, Hadoop lab > Hadoop lab capacity")
EOF
