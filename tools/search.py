"""
The online half of the search engine: load the index into memory, then answer
queries with BM25 ranking in milliseconds. Uses the serving copy that
jobs/build_search_index_spark.sh fetched into data/search/.

    python3 tools/search.py "memory leak"          one query
    python3 tools/search.py                        interactive: type queries, blank to quit
"""
import math
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent / "data" / "search"
K1, B = 1.2, 0.75   # BM25: k1 = how fast repeats stop helping, b = length normalisation
# Must match the analysis in build_search_index_spark.py exactly.
STOP = {"the", "and", "of", "to", "a", "in", "is", "it", "for", "that", "this", "on",
        "be", "are", "as", "with", "by", "or", "an", "if", "not", "you", "from", "at",
        "can", "will", "was", "which", "all", "but", "have", "has", "no", "so", "do",
        "we", "its", "there", "use", "may", "any", "these", "other", "than", "also"}


def analyse(text):
    return [t for t in re.split(r"[^a-z0-9]+", text.lower()) if len(t) >= 2 and t not in STOP]


def load():
    t0 = time.perf_counter()
    index = {}
    with open(HERE / "postings.tsv", encoding="utf-8") as f:
        for line in f:
            term, idf, plist = line.rstrip("\n").split("\t")
            index[term] = (float(idf), [(int(d), int(tf)) for d, tf in
                                        (p.split(":") for p in plist.split(","))])
    docs = {}
    with open(HERE / "docs.tsv", encoding="utf-8") as f:
        for line in f:
            doc_id, length, path, preview = (line.rstrip("\n").split("\t") + [""])[:4]
            docs[int(doc_id)] = (int(length), path, preview)
    avgdl = sum(d[0] for d in docs.values()) / len(docs)
    postings = sum(len(p) for _, p in index.values())
    print(f"loaded {len(index):,} terms, {postings:,} postings, {len(docs):,} documents "
          f"in {time.perf_counter() - t0:.2f} s")
    return index, docs, avgdl


def search(query, index, docs, avgdl, k=10):
    t0 = time.perf_counter()
    terms = analyse(query)
    scores = {}
    for term in terms:
        if term not in index:
            continue
        idf, plist = index[term]
        for doc_id, tf in plist:
            dl = docs[doc_id][0]
            scores[doc_id] = scores.get(doc_id, 0.0) + idf * tf * (K1 + 1) / (tf + K1 * (1 - B + B * dl / avgdl))
    top = sorted(scores.items(), key=lambda x: -x[1])[:k]
    ms = (time.perf_counter() - t0) * 1000

    print(f'\nquery "{query}" -> terms {terms}')
    for term in terms:
        if term in index:
            print(f"  {term:<16} idf {index[term][0]:5.2f}   in {len(index[term][1]):>4} documents")
        else:
            print(f"  {term:<16} not in the index")
    print(f"{len(scores):,} matching documents, top {len(top)} in {ms:.2f} ms")
    for rank, (doc_id, score) in enumerate(top, 1):
        _, path, preview = docs[doc_id]
        print(f"{rank:>3}. {score:6.2f}  {path.replace('/usr/share/doc/', '')}")
        print(f"            {preview[:90]}")


if __name__ == "__main__":
    index, docs, avgdl = load()
    if len(sys.argv) > 1:
        search(" ".join(sys.argv[1:]), index, docs, avgdl)
    else:
        while (q := input("\nsearch> ").strip()):
            search(q, index, docs, avgdl)
