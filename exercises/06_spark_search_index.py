"""
06  Spark: word count, an inverted index and TF-IDF search over HDFS.
Needs exercise 04's corpus in HDFS (/lab/corpus).

Run:  docker compose exec spark /opt/spark/bin/spark-submit --master "local[2]" /exercises/06_spark_search_index.py "patent warranty"
"""
import math
import re
import sys
import time

from pyspark.sql import SparkSession

QUERY = (sys.argv[1] if len(sys.argv) > 1 else "patent warranty").lower().split()

spark = SparkSession.builder.appName("lab-search-index").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

# (doc, text) pairs. One HDFS file per document.
docs = sc.wholeTextFiles("hdfs://namenode:8020/lab/corpus").map(
    lambda kv: (kv[0].rsplit("/", 1)[-1], kv[1]))
words = docs.flatMap(
    lambda d: [(d[0], w) for w in re.findall(r"[a-z]{3,}", d[1].lower())]).cache()

t = time.time()
counts = words.map(lambda dw: (dw[1], 1)).reduceByKey(lambda a, b: a + b)
print("\n== Word count, top 5:", counts.takeOrdered(5, key=lambda kv: -kv[1]))

# Inverted index: word -> list of (doc, count). The core data structure of a search engine.
tf = words.map(lambda dw: (dw, 1)).reduceByKey(lambda a, b: a + b)        # ((doc, word), n)
index = tf.map(lambda x: (x[0][1], (x[0][0], x[1]))).groupByKey().mapValues(list)
print("== Inverted index size:", index.count(), "distinct terms")

# TF-IDF: rare terms count for more than common ones.
n_docs = docs.count()
df = index.mapValues(len).collectAsMap()
idf = {w: math.log((1 + n_docs) / (1 + d)) + 1 for w, d in df.items()}
scores = (index.filter(lambda kv: kv[0] in QUERY)
          .flatMap(lambda kv: [(doc, n * idf[kv[0]]) for doc, n in kv[1]])
          .reduceByKey(lambda a, b: a + b))
print("== Query", QUERY, "top 5 documents by TF-IDF:", scores.takeOrdered(5, key=lambda kv: -kv[1]))
print(f"== Took {time.time() - t:.1f} s over {n_docs} documents")

print("""
Talking points
- Spark keeps intermediate data in memory (cache) and plans the whole job as a
  DAG, so multi-step work like this avoids MapReduce's disk write per stage.
- reduceByKey combines on each node before the shuffle, like a MapReduce combiner.
- A search engine at scale: build the index offline (batch) or incrementally,
  shard it (by document: every shard answers every query, results merged; or by
  term: each query hits fewer shards but hot terms create hot spots), replicate
  shards for query throughput and availability, cache popular queries.
- Ranking: TF-IDF here; production systems use BM25 plus many other signals.
""")
spark.stop()
