# Hadoop and Spark lab

A small cluster running a real data pipeline, for practising big data and
capacity planning: HDFS (1 NameNode, 4 DataNodes), YARN (1 ResourceManager, 2
NodeManagers), an edge node, a Spark client, and Prometheus with Grafana
watching all of it. Uses about 5 GB of memory. Runs the same on a Linux server
or a laptop with Docker.

The use case: turn 3.5 million real web server requests into a capacity report.

## Layout

```
run_pipeline.sh                  the whole pipeline, end to end
jobs/                            the data pipeline
  ingest_web_logs.sh             HDFS: land raw logs in the data lake
  busiest_hours_mapreduce.sh     MapReduce on YARN: busiest hours, status codes
  busiest_hours_spark.sh/.py     the same job in Spark, for comparison
  compare_engines.sh             runs both, times them, checks the answers match
  collect_docs.sh                search: pack a document corpus into HDFS
  build_search_index_spark.sh/.py  search: build the inverted index on YARN
  capacity_report_spark.sh       Spark on YARN: submits the capacity report
  capacity_report_spark.py       the Spark job itself
ops/                             running the cluster
  bootstrap_cluster.sh           one-time setup: HDFS folders, Spark libraries
  hdfs_health.sh                 reported vs real capacity, block health
  datanode_failure_drill.sh      stop a DataNode, watch HDFS repair itself
  benchmark_throughput.sh        how many MB/s the cluster writes and sorts
  metrics_snapshot.sh            key numbers from Prometheus, in the terminal
tools/
  hdfs_capacity_planner.py       nodes, disk and NameNode memory for a target
  search.py                      search: serve queries from the index in memory
  sharded_search.py              search at scale: document shards, fan-out, tail latency
monitoring/                      Prometheus scrape config, Grafana dashboard
spark-conf/spark-defaults.conf   cluster-wide Spark settings (YARN, cluster mode)
hadoop-conf/                     cluster client config for the Spark containers
images/hadoop-worker/            NodeManager image: Hadoop plus Python
data/                            input logs (not in git, see below)
```

Every script runs on the host, from the lab folder, and sends the work to the
edge node or the Spark client, as a scheduler would on a real cluster.

## Production setup

The lab runs Spark the way a production cluster does:

| Practice | Where |
|---|---|
| Cluster deploy mode: the driver runs in a YARN container, not on the client | `spark-conf/spark-defaults.conf` |
| Shared settings in one defaults file; each job's script holds only its sizing | `spark-conf/`, `jobs/*_spark.sh` |
| Worker image with the same Python on every NodeManager, for PySpark drivers and executors | `images/hadoop-worker/` |
| Spark's libraries stored in HDFS once (`spark.yarn.archive`), not uploaded per run | `ops/bootstrap_cluster.sh` |
| Results and reports written to HDFS, since a cluster-mode driver has no terminal | `/lake/reports` |
| Spark History Server for finished applications | http://localhost:18080 |
| YARN log aggregation: `yarn logs -applicationId <id>` after a job ends | `config` |

The worker image needs a portable Python: an unpacked
[python-build-standalone](https://github.com/astral-sh/python-build-standalone)
release, passed as `PYTHON_DIST` (default: the one `uv` installs under
`~/.local/share/uv/python`). Build it with `docker compose build nodemanager1`.

## Start, stop, reset

```bash
docker compose up -d            # start (first time pulls about 4 GB of images)
docker compose ps               # all 13 containers should be Up
docker compose down             # stop and WIPE all HDFS data (by design);
                                # Prometheus history is kept in a volume
```

Web UIs are on localhost only. From a laptop, forward the ports (VS Code Ports
tab, or `ssh -L 13000:localhost:13000 -L 9870:localhost:9870 -L 18088:localhost:18088 <host>`):

| UI | URL |
|---|---|
| Grafana dashboard | http://localhost:13000, Hadoop lab > Hadoop lab capacity (admin / admin on first login) |
| Prometheus | http://localhost:19090, Status > Targets |
| Spark History Server | http://localhost:18080 |
| NameNode (HDFS) | http://localhost:9870 |
| ResourceManager (YARN jobs) | http://localhost:18088 |

## Get the data

The NASA Kennedy Space Center web server logs for July and August 1995, about
3.5 million requests, published by the Internet Traffic Archive at Lawrence
Berkeley National Laboratory:

```bash
mkdir -p data && cd data
curl -fLO https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
curl -fLO https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
cd ..
```

## Run it

Open Grafana first (Last 30 minutes, refresh 10s) to watch each stage, then:

```bash
bash run_pipeline.sh            # about 8 minutes
```

| Stage | Script | What it does | Where to watch |
|---|---|---|---|
| 1 | `jobs/ingest_web_logs.sh` | Lands the logs in `/lake/raw`, one folder per month | Grafana: HDFS used, NameNode objects |
| 2 | `jobs/busiest_hours_mapreduce.sh` | Counts requests per hour and per status code from the raw logs | YARN UI, Grafana: YARN panels |
| 3 | `jobs/capacity_report_spark.sh` | Cleans every request into `/lake/curated` (Parquet, one file per day), builds `/lake/marts/nasa_hourly`, prints the capacity report | YARN UI, Grafana: YARN panels |
| 4 | `ops/metrics_snapshot.sh` | Reads the cluster's numbers from Prometheus | Terminal |

Each step also runs on its own. To see MapReduce and Spark do the same job:

```bash
bash jobs/compare_engines.sh    # about 5 minutes
```

On this data MapReduce needs 4 YARN applications and about 150 s, Spark 1
application and about 95 s, with identical answers. MapReduce writes every
stage to disk and reads the logs once per question; Spark reads them once and
keeps them in memory.

Then try the operations scripts:

```bash
bash ops/hdfs_health.sh             # capacity and block health
bash ops/datanode_failure_drill.sh  # watch Grafana: Live DataNodes, Blocks needing attention
bash ops/benchmark_throughput.sh    # write and sort throughput, in MB/s
python3 tools/hdfs_capacity_planner.py
```

## Search engine

The two halves of a search engine: an offline batch job builds the index, an
online server answers queries from memory.

```bash
bash jobs/collect_docs.sh                 # 450 documents from /usr/share/doc, one JSON-lines file
bash jobs/build_search_index_spark.sh     # Spark on YARN: inverted index, BM25 statistics
python3 tools/search.py "memory leak"     # BM25-ranked results in about 1 ms
python3 tools/search.py                   # interactive
python3 tools/sharded_search.py shards    # split into document shards, fan out, merge
python3 tools/sharded_search.py tail      # fan-out and tail latency, hedged requests (simulated)
```

The index (65,766 terms, 307,407 postings) is written to `/lake/search` as
Parquet, and as a compact serving copy that `tools/search.py` loads into memory.

## What the data shows

- **The peak is an event.** The busiest minute (405 requests, 9.5 times the
  average) and the six busiest hours all fall on 13 July 1995, the day of a
  Space Shuttle launch. Size for the peak and plan for known events.
- **Gaps are not always outages.** Two gaps with no requests: one where the July
  file ends early, one a real shutdown. The logs alone cannot tell which.
- **gzip limits parallelism.** A .gz file cannot be split, so each month is read
  by one task however large the cluster.
- **Small files appear by accident.** Writing Spark output partitioned by date
  without repartitioning made about 700 files of 160 KB; one file per day
  brought it to 79.

## A capacity trap this lab shows

`hdfs dfsadmin -report` says the cluster has about 156 GB. It has nothing like
that: all four DataNodes write to the same Docker disk, and HDFS counts that disk
four times. On real clusters the same mistake appears when DataNodes share a SAN
or virtual disks on one array. Always check what the reported capacity sits on
(`ops/hdfs_health.sh` shows both).

## Concepts to be ready for

**HDFS.** NameNode keeps metadata in memory, DataNodes keep blocks. 128 MB
blocks, 3 replicas, rack awareness. Writes go through a pipeline of DataNodes.
NameNode is the scaling limit (object count) and a single point of failure,
solved by HA (active and standby NameNodes with JournalNodes) and federation
(several NameNodes, each owning part of the namespace).

**YARN.** The ResourceManager shares the cluster's memory and CPU between jobs;
each NodeManager runs containers on its machine. Every job gets an
ApplicationMaster that asks for containers. Queues divide capacity between
teams. Waiting containers mean the cluster is saturated.

**MapReduce.** Map, shuffle and sort, reduce. Data locality: move the
computation to the data. Combiners cut shuffle traffic. Every stage writes to
disk: robust, slow for iterative work.

**Spark.** In-memory, DAG of stages split at shuffles, lazy evaluation, RDDs and
DataFrames, partitions as the unit of parallelism. Sizing: executors, cores per
executor, memory per executor, and partitions about 2 to 4 times total cores.
Watch for skew (one partition far larger than the rest).

**Monitoring.** Prometheus scrapes each daemon's metrics every 15 s and keeps
the history; Grafana draws it. History is what forecasting needs: single
readings only tell you about now.

**Scaling a system.** Vertical then horizontal; partition (shard) by a key that
spreads load; replicate for availability and read throughput; cache hot data;
make work asynchronous with queues; find the bottleneck before adding capacity.

**Capacity planning method.** Measure the current load and growth, forecast,
add headroom for failure and peaks, check every resource (disk, memory, CPU,
network, IOPS, NameNode objects), set triggers with procurement lead time, then
compare forecast against actual every month.
