"""
08b  Capacity report from real web server logs (NASA Kennedy Space Center,
July and August 1995, about 3.5 million requests). Runs on YARN.

    docker compose exec spark /opt/spark/bin/spark-submit \
        --master yarn --deploy-mode client \
        --num-executors 3 --executor-cores 1 --executor-memory 640m \
        --conf spark.yarn.am.memory=512m --conf spark.driver.host=spark \
        --conf spark.sql.shuffle.partitions=12 --conf spark.sql.session.timeZone=UTC \
        /exercises/08_web_logs_capacity.py

Only DataFrame operations run on the cluster: they execute in Spark's JVM, so the
NodeManagers need no Python. The small aggregated results are summarised on the
driver.

Layout it builds:
    /lake/raw/nasa_http/month=...        the files as delivered (08_load_web_logs.sh)
    /lake/curated/nasa_requests/date=... one row per request, Parquet
    /lake/marts/nasa_hourly              requests, errors and bytes per hour
"""
from datetime import timedelta

from pyspark.sql import SparkSession, functions as F

LAKE = "hdfs://namenode:8020/lake"
TARGET_UTILISATION = 0.70   # size so the busiest minute runs at 70% of capacity

spark = SparkSession.builder.appName("nasa-web-logs-capacity").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- Parse --------------------------------------------------------------------
# host ident user [01/Jul/1995:00:00:01 -0400] "GET /path HTTP/1.0" 200 6245
LINE = (r'^(\S+) \S+ \S+ \[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}) [-+]\d{4}\] '
        r'"(?:(\S+) )?(\S+?)(?: \S+)?" (\d{3}) (\S+)$')

raw = spark.read.text(f"{LAKE}/raw/nasa_http")
parsed = raw.select(
    F.regexp_extract("value", LINE, 1).alias("host"),
    F.regexp_extract("value", LINE, 2).alias("ts_text"),
    F.regexp_extract("value", LINE, 3).alias("method"),
    F.regexp_extract("value", LINE, 4).alias("path"),
    F.regexp_extract("value", LINE, 5).alias("status_text"),
    F.regexp_extract("value", LINE, 6).alias("bytes_text"),
)
lines_total = parsed.count()

# Server local time (US Eastern, -0400) is kept as is: the daily cycle is about
# the server's own day, so no time zone shift.
requests = (
    parsed.where(F.col("status_text") != "")
    .select(
        "host", "method", "path",
        F.to_timestamp("ts_text", "dd/MMM/yyyy:HH:mm:ss").alias("ts"),
        F.col("status_text").cast("int").alias("status"),
        F.when(F.col("bytes_text") == "-", 0)
         .otherwise(F.col("bytes_text").cast("long")).alias("bytes"),
    )
    .withColumn("date", F.to_date("ts"))
    .repartition(12)
    .cache()
)
ok_total = requests.count()

(requests.write.mode("overwrite").partitionBy("date")
 .parquet(f"{LAKE}/curated/nasa_requests"))

# --- Load per minute --------------------------------------------------------------
per_minute = requests.groupBy(F.date_trunc("minute", "ts").alias("minute")).agg(
    F.count("*").alias("n"), F.sum("bytes").alias("bytes"))
m = per_minute.agg(
    F.max("n").alias("max"), F.avg("n").alias("avg"),
    F.expr("percentile_approx(n, 0.95)").alias("p95"),
    F.expr("percentile_approx(n, 0.99)").alias("p99"),
    F.max("bytes").alias("max_bytes")).first()
peak_minute = per_minute.orderBy(F.desc("n")).first()

# --- Hourly mart, then the rest on the driver (about 1,500 rows) ---------------------
hourly = requests.groupBy(F.date_trunc("hour", "ts").alias("hour")).agg(
    F.count("*").alias("requests"),
    F.sum(F.when(F.col("status") >= 500, 1).otherwise(0)).alias("errors_5xx"),
    F.sum(F.when(F.col("status").between(400, 499), 1).otherwise(0)).alias("errors_4xx"),
    F.sum("bytes").alias("bytes"),
).orderBy("hour")
hourly.write.mode("overwrite").parquet(f"{LAKE}/marts/nasa_hourly")
hours = {r["hour"]: r for r in hourly.collect()}

status_mix = {r["cls"]: r["n"] for r in requests.groupBy(
    (F.floor(F.col("status") / 100) * 100).cast("int").alias("cls")).count()
    .withColumnRenamed("count", "n").collect()}
top_paths = (requests.groupBy("path").agg(F.count("*").alias("n"), F.sum("bytes").alias("bytes"))
             .orderBy(F.desc("n")).limit(8).collect())

# Fill the calendar so silent hours show up as zero, not as missing rows.
first, last = min(hours), max(hours)
calendar, t = [], first
while t <= last:
    calendar.append((t, hours[t]["requests"] if t in hours else 0))
    t += timedelta(hours=1)

gaps, run = [], []
for t, n in calendar:
    if n == 0:
        run.append(t)
    elif run:
        gaps.append((run[0], run[-1] + timedelta(hours=1))); run = []
outages = [(a, b) for a, b in gaps if (b - a) >= timedelta(hours=2)]
outage_hours = {a + timedelta(hours=i) for a, b in outages
                for i in range(int((b - a).total_seconds() // 3600))}

daily = {}
for t, n in calendar:
    daily.setdefault(t.date(), []).append((t, n))
full_days = {d: sum(n for _, n in v) for d, v in daily.items()
             if len(v) == 24 and not any(t in outage_hours for t, _ in v)}

by_hour_of_day = {}
for t, n in calendar:
    if t not in outage_hours:
        by_hour_of_day.setdefault(t.hour, []).append(n)
profile = {h: sum(v) / len(v) for h, v in by_hour_of_day.items()}
busiest_hod = max(profile, key=profile.get)
quietest_hod = min(profile, key=profile.get)

weekday = [n for d, n in full_days.items() if d.weekday() < 5]
weekend = [n for d, n in full_days.items() if d.weekday() >= 5]

# Do errors rise with load? Correlation of hourly 5xx rate against hourly requests.
pairs = [(r["requests"], r["errors_5xx"] / r["requests"]) for r in hours.values() if r["requests"]]
n_ = len(pairs)
mx = sum(a for a, _ in pairs) / n_
my = sum(b for _, b in pairs) / n_
sxy = sum((a - mx) * (b - my) for a, b in pairs)
sxx = sum((a - mx) ** 2 for a, _ in pairs)
syy = sum((b - my) ** 2 for _, b in pairs)
corr = sxy / (sxx * syy) ** 0.5 if sxx and syy else 0.0

# Trend: least-squares line through the daily totals of full days.
xs = sorted(full_days)
x0 = xs[0]
X = [(d - x0).days for d in xs]
Y = [full_days[d] for d in xs]
xm, ym = sum(X) / len(X), sum(Y) / len(Y)
slope = sum((a - xm) * (b - ym) for a, b in zip(X, Y)) / sum((a - xm) ** 2 for a in X)

# --- Report -----------------------------------------------------------------------
peak_rps = m["max"] / 60
mbit = m["max_bytes"] * 8 / 60 / 1e6
plan_rps = peak_rps / TARGET_UTILISATION
p99_rps = m["p99"] / 60

print(f"""
=====================================================================
 CAPACITY REPORT  NASA web server, {first:%d %b} to {last:%d %b %Y}
=====================================================================
Data
  lines read            {lines_total:>12,}
  parsed requests       {ok_total:>12,}   ({lines_total - ok_total:,} malformed, skipped)

Load per minute
  average               {m['avg']:>12,.0f} requests
  p95 / p99             {m['p95']:>12,} / {m['p99']:,}
  peak                  {m['max']:>12,}   at {peak_minute['minute']:%a %d %b %H:%M}  = {peak_rps:.1f} req/s
  peak-to-average       {m['max'] / m['avg']:>12.1f} x
  peak bandwidth        {mbit:>12.2f} Mbit/s (busiest minute by bytes)

Daily pattern (server local time, outage hours excluded)
  busiest hour of day   {busiest_hod:>02}:00  ({profile[busiest_hod]:,.0f} req/h on average)
  quietest hour of day  {quietest_hod:>02}:00  ({profile[quietest_hod]:,.0f} req/h)
  weekday vs weekend    {sum(weekday) / len(weekday):,.0f} vs {sum(weekend) / len(weekend):,.0f} requests a day

Status codes
  """ + "  ".join(f"{c}xx: {status_mix[c] / ok_total:.1%}" for c in sorted(status_mix)).replace("00xx", "xx") + f"""
  5xx rate vs load      correlation {corr:+.2f} per hour  (near 0: errors do not rise with load)

Gaps of 2 hours or more with no requests (an outage or missing logs:
 the logs alone cannot tell which)""")
for a, b in outages:
    print(f"  {a:%a %d %b %H:%M} to {b:%a %d %b %H:%M}  ({(b - a).total_seconds() / 3600:.0f} h)")
if not outages:
    print("  none")
print(f"""
Trend over {len(full_days)} full days
  {slope:+,.0f} requests a day per day  ({slope / ym:+.2%} of the average day)

Busiest pages""")
for r in top_paths:
    print(f"  {r['n']:>9,}  {r['bytes'] / 1e9:>6.2f} GB  {r['path'][:60]}")
print(f"""
Sizing
  Size for the peak, not the typical minute: the busiest minute ({peak_rps:.1f} req/s)
  at {TARGET_UTILISATION:.0%} utilisation needs about {plan_rps:.1f} req/s of capacity.
  The p99 minute ({p99_rps:.1f} req/s) would then run at {p99_rps / plan_rps:.0%}, and the
  average at {m['avg'] / 60 / plan_rps:.0%}: the price of absorbing a {m['max'] / m['avg']:.0f}x spike.

Written to HDFS
  {LAKE}/curated/nasa_requests   (Parquet, partitioned by date)
  {LAKE}/marts/nasa_hourly       (Parquet, {len(hours):,} hours)
=====================================================================""")
spark.stop()
