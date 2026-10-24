"""
07  Capacity planning calculator for an HDFS cluster.
Change the inputs and rerun. Pure Python, no cluster needed.

Run:  docker compose exec spark python3 /exercises/07_capacity_calculator.py
"""
# ---- inputs ----------------------------------------------------------------
daily_ingest_tb   = 2.0     # new raw data per day
growth_per_month  = 0.05    # ingest grows 5% a month
retention_days    = 365
compression_ratio = 3.0     # e.g. Parquet with Snappy or ZSTD
replication       = 3
temp_overhead     = 0.25    # shuffle, staging and intermediate data
target_fill       = 0.70    # plan to run at 70%, alert at 80%
disk_per_node_tb  = 12 * 8  # 12 disks of 8 TB
avg_file_mb       = 256
block_mb          = 128
horizon_months    = 12
# ----------------------------------------------------------------------------

def stored_tb(months_ahead):
    """Logical data held at a point in time: the last retention_days of ingest."""
    daily = daily_ingest_tb * (1 + growth_per_month) ** months_ahead
    return daily * retention_days / compression_ratio

print(f"{'month':>5} {'logical TB':>11} {'raw TB':>9} {'nodes':>6} {'blocks (M)':>11} {'NN heap GB':>11}")
for m in range(0, horizon_months + 1, 3):
    logical = stored_tb(m)
    raw = logical * replication * (1 + temp_overhead)
    nodes = raw / (disk_per_node_tb * target_fill)
    files = logical * 1024 * 1024 / avg_file_mb
    blocks = files * max(1, avg_file_mb / block_mb)
    heap = (files + blocks) * 150 / 1e9 * 2          # 150 bytes per object, x2 for GC headroom
    print(f"{m:>5} {logical:>11.0f} {raw:>9.0f} {nodes:>6.0f} {blocks/1e6:>11.1f} {heap:>11.1f}")

print("""
How to talk through it
1. Start from the demand: ingest rate, growth, retention, compression.
2. Multiply out: replication x temporary space, then divide by a target fill
   level (not 100%) so a failed node can be re-replicated and growth has room.
3. Check every dimension, not only disk: NameNode heap (object count), network
   (re-replication and shuffle), CPU and memory for compute (YARN containers),
   and IOPS. The tightest one sets the node count.
4. Add lead time: if hardware takes 8 weeks to arrive, the trigger to order is
   "projected to hit 80% within 8 weeks plus a safety margin".
5. Revisit with measured growth each month and compare forecast against actual.
Levers: better compression, erasure coding instead of 3x replication for cold
data (about 1.5x overhead), tiered storage, shorter retention, compaction.
""")
