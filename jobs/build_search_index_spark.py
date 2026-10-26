"""
Build an inverted index with BM25 statistics from the document corpus.
Offline indexing, the batch half of a search engine. Submit with
jobs/build_search_index_spark.sh.

Analysis (applied to documents here and to queries in tools/search.py, identically):
lower-case, split on anything that is not a letter or digit, drop stop words and
terms shorter than 2 characters. No stemming, to keep it readable.

Writes to HDFS:
    /lake/search/postings   term, doc_id, tf               (Parquet: the inverted index)
    /lake/search/terms      term, df, idf                   (Parquet: the dictionary)
    /lake/search/docs       doc_id, path, length, preview   (Parquet: the document store)
    /lake/search/serving/   the same, as compact text files for the in-memory server
    /lake/reports/search_index   this job's report
"""
import contextlib
import io

from pyspark.sql import SparkSession, functions as F

LAKE = "hdfs://namenode:8020/lake"
STOP = ["the", "and", "of", "to", "a", "in", "is", "it", "for", "that", "this", "on",
        "be", "are", "as", "with", "by", "or", "an", "if", "not", "you", "from", "at",
        "can", "will", "was", "which", "all", "but", "have", "has", "no", "so", "do",
        "we", "its", "there", "use", "may", "any", "these", "other", "than", "also"]

spark = SparkSession.builder.appName("build-search-index").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

docs = spark.read.json(f"{LAKE}/raw/docs/corpus.jsonl").repartition(6).cache()
n_docs = docs.count()

# --- Analyse: one row per (document, term occurrence) -------------------------------
tokens = (docs
          .select("doc_id", F.explode(F.split(F.lower("text"), "[^a-z0-9]+")).alias("term"))
          .where((F.length("term") >= 2) & (~F.col("term").isin(STOP))))

# --- Inverted index: term frequency per document --------------------------------------
postings = tokens.groupBy("term", "doc_id").agg(F.count("*").alias("tf")).cache()
lengths = tokens.groupBy("doc_id").agg(F.count("*").alias("length"))

# --- Dictionary: document frequency and BM25 idf per term ------------------------------
terms = (postings.groupBy("term").agg(F.count("*").alias("df"))
         .withColumn("idf", F.log(1 + (F.lit(n_docs) - F.col("df") + 0.5) / (F.col("df") + 0.5))))

doc_store = (docs.join(lengths, "doc_id")
             .select("doc_id", "path", "length",
                     F.regexp_replace(F.substring("text", 1, 160), r"\s+", " ").alias("preview")))

# Index sorted by term, so each term's posting list is stored together.
postings.repartition(4, "term").sortWithinPartitions("term", "doc_id") \
    .write.mode("overwrite").parquet(f"{LAKE}/search/postings")
terms.write.mode("overwrite").parquet(f"{LAKE}/search/terms")
doc_store.write.mode("overwrite").parquet(f"{LAKE}/search/docs")

# --- Serving copy: one line per term with its whole posting list ----------------------
# term <TAB> idf <TAB> doc:tf,doc:tf,...   and   doc_id <TAB> length <TAB> path <TAB> preview
(postings.groupBy("term")
 .agg(F.concat_ws(",", F.collect_list(F.concat_ws(":", "doc_id", "tf"))).alias("plist"))
 .join(terms, "term")
 .select(F.concat_ws("\t", "term", F.format_number("idf", 4).cast("string"), "plist"))
 .coalesce(1).write.mode("overwrite").text(f"{LAKE}/search/serving/postings"))
(doc_store.select(F.concat_ws("\t", "doc_id", "length", "path", "preview"))
 .coalesce(1).write.mode("overwrite").text(f"{LAKE}/search/serving/docs"))

# --- Report ---------------------------------------------------------------------------
st = postings.agg(F.count("*").alias("postings"), F.sum("tf").alias("tokens")).first()
vocab = terms.count()
avg_len = lengths.agg(F.avg("length")).first()[0]
top_df = terms.orderBy(F.desc("df")).limit(8).collect()
rare = terms.where(F.col("df") == 1).count()

report = io.StringIO()
with contextlib.redirect_stdout(report):
    print(f"""
=====================================================================
 SEARCH INDEX BUILD
=====================================================================
Corpus
  documents             {n_docs:>12,}
  terms after analysis  {st['tokens']:>12,}   (average {avg_len:,.0f} per document)

Index
  vocabulary            {vocab:>12,} distinct terms
  postings              {st['postings']:>12,} (term, document) pairs
  terms in one document {rare:>12,}   ({rare / vocab:.0%} of the vocabulary)

Most common terms (longest posting lists, lowest idf)""")
    for r in top_df:
        print(f"  {r['term']:<16} in {r['df']:>4} of {n_docs} documents   idf {r['idf']:.2f}")
    print(f"""
Written to HDFS
  {LAKE}/search/postings, terms, docs   (Parquet)
  {LAKE}/search/serving/                (for tools/search.py)
=====================================================================""")
text = report.getvalue()
print(text)
(spark.createDataFrame([(line,) for line in text.splitlines()], "value string")
 .coalesce(1).write.mode("overwrite").text(f"{LAKE}/reports/search_index"))
spark.stop()
