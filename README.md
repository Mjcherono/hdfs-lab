# Hadoop and Spark lab

A small cluster for practising big data and capacity planning concepts:
HDFS (1 NameNode, 4 DataNodes), YARN (1 ResourceManager, 2 NodeManagers) and a
Spark client. Uses about 4 GB of memory. Runs the same on a Linux server or a laptop with
Docker.

## Start, stop, reset

```bash
docker compose up -d            # start (first time pulls about 3 GB of images)
docker compose ps               # all 9 containers should be Up
docker compose down             # stop and WIPE all HDFS data (by design)
```

Web UIs are on localhost only. From a laptop, forward the ports (VS Code Ports
tab, or `ssh -L 9870:localhost:9870 -L 18088:localhost:18088 <host>`):

| UI | URL |
|---|---|
| NameNode (HDFS) | http://localhost:9870 |
| ResourceManager (YARN jobs) | http://localhost:18088 |

## Exercises

Run them in order. Each prints what to look at and the points to make in an
interview.

| # | Topic | Run |
|---|---|---|
| 01 | Blocks and replicas | `docker compose exec namenode bash /exercises/01_blocks_and_replicas.sh` |
| 02 | DataNode failure and re-replication | `bash exercises/02_datanode_failure_HOST.sh` (on the host) |
| 03 | Small-files problem, NameNode memory | `docker compose exec namenode bash /exercises/03_small_files.sh` |
| 04 | MapReduce word count on YARN | `docker compose exec resourcemanager bash /exercises/04_mapreduce_wordcount.sh` |
| 05 | TeraGen and TeraSort benchmark | `docker compose exec resourcemanager bash /exercises/05_terasort.sh` |
| 06 | Spark: inverted index and TF-IDF search | `docker compose exec spark /opt/spark/bin/spark-submit --master "local[2]" /exercises/06_spark_search_index.py "patent warranty"` |
| 07 | Capacity planning calculator | `docker compose exec spark python3 /exercises/07_capacity_calculator.py` |

## A capacity trap this lab shows

`hdfs dfsadmin -report` says the cluster has about 156 GB. It has nothing like
that: all four DataNodes write to the same Docker disk, and HDFS counts that disk
four times. On real clusters the same mistake appears when DataNodes share a SAN
or virtual disks on one array. Always check what the reported capacity sits on.
On the server this was built on, that disk is a 40 GB Docker volume, so keep test data small.

## Concepts to be ready for

**HDFS.** NameNode keeps metadata in memory, DataNodes keep blocks. 128 MB
blocks, 3 replicas, rack awareness. Writes go through a pipeline of DataNodes.
NameNode is the scaling limit (object count) and a single point of failure,
solved by HA (active and standby NameNodes with JournalNodes) and federation
(several NameNodes, each owning part of the namespace).

**MapReduce.** Map, shuffle and sort, reduce. Data locality: move the
computation to the data. Combiners cut shuffle traffic. Every stage writes to
disk: robust, slow for iterative work.

**Spark.** In-memory, DAG of stages split at shuffles, lazy evaluation, RDDs and
DataFrames, partitions as the unit of parallelism. Sizing: executors, cores per
executor, memory per executor, and partitions about 2 to 4 times total cores.
Watch for skew (one partition far larger than the rest).

**Scaling a system.** Vertical then horizontal; partition (shard) by a key that
spreads load; replicate for availability and read throughput; cache hot data;
make work asynchronous with queues; find the bottleneck before adding capacity.

**Search engine.** Crawl, parse, build an inverted index (term to documents),
rank (TF-IDF, BM25, link and quality signals), serve. Shard the index by
document or by term, replicate shards, merge results, cache popular queries.
Index updates: batch rebuild plus incremental segments, merged in the
background.

**Capacity planning method.** Measure the current load and growth, forecast,
add headroom for failure and peaks, check every resource (disk, memory, CPU,
network, IOPS, NameNode objects), set triggers with procurement lead time, then
compare forecast against actual every month.
