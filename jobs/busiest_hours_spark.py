"""
The same job as busiest_hours_mapreduce.sh, done by Spark: requests per hour and
per HTTP status code, from the raw logs, with the same regular expressions.
Writes tab-separated "count<TAB>key" files, sorted by count, like MapReduce does,
so the two results can be compared line for line.
Submit with jobs/busiest_hours_spark.sh.
"""
from pyspark.sql import SparkSession, functions as F

LAKE = "hdfs://namenode:8020/lake"
HOUR = r"\[([0-9]{2}/[A-Za-z]{3}/[0-9]{4}:[0-9]{2})"
STATUS = r'" ([0-9]{3}) '

spark = SparkSession.builder.appName("busiest-hours-spark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read once, keep in memory: both counts reuse it. MapReduce reads the files twice.
lines = spark.read.text(f"{LAKE}/raw/nasa_http").cache()

for name, pattern in (("busiest_hours", HOUR), ("status_codes", STATUS)):
    (lines.select(F.regexp_extract("value", pattern, 1).alias("key"))
     .where(F.col("key") != "")
     .groupBy("key").count()
     .orderBy(F.desc("count"), "key")
     .select("count", "key")
     .coalesce(1)
     .write.mode("overwrite").option("sep", "\t").csv(f"{LAKE}/marts/spark_{name}"))

spark.stop()
