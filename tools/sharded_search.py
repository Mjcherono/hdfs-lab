"""
Scaling the search engine: sharding by document, fan-out, merge, and tail latency.
Uses the serving copy of the index in data/search/ (jobs/build_search_index_spark.sh).

    python3 tools/sharded_search.py shards    split the index into document shards, fan
                                              queries out, merge, compare with one index
    python3 tools/sharded_search.py tail      fan-out and tail latency, with and without
                                              hedged requests (a simulation, see below)
"""
import random
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from search import B, K1, analyse, load  # noqa: E402

QUERIES = ["memory leak", "ssl certificate verification", "kernel panic",
           "python3 deprecated", "unicode encoding error", "thread safe"]


# --- Part 1: real sharding ------------------------------------------------------------
def build_shards(index, docs, n):
    """Split by document: shard = doc_id % n. Each shard is a complete small index of
    its own documents, with idf computed from what that shard alone can see."""
    shards = []
    for s in range(n):
        mine = {d for d in docs if d % n == s}
        sidx = defaultdict(list)
        for term, (_, plist) in index.items():
            for d, tf in plist:
                if d in mine:
                    sidx[term].append((d, tf))
        avgdl = sum(docs[d][0] for d in mine) / len(mine)
        shards.append({"docs": mine, "postings": sidx, "n": len(mine), "avgdl": avgdl})
    return shards


def shard_search(shard, terms, docs, k, global_stats=None):
    """global_stats = (idf per term, average document length) for the whole corpus;
    without it the shard scores with what it alone can see."""
    global_idf, avgdl = global_stats if global_stats else (None, shard["avgdl"])
    scores = {}
    for t in terms:
        plist = shard["postings"].get(t)
        if not plist:
            continue
        if global_idf is not None:
            idf = global_idf[t]
        else:  # local idf: only this shard's documents
            df = len(plist)
            idf = __import__("math").log(1 + (shard["n"] - df + 0.5) / (df + 0.5))
        for d, tf in plist:
            dl = docs[d][0]
            scores[d] = scores.get(d, 0.0) + idf * tf * (K1 + 1) / (tf + K1 * (1 - B + B * dl / avgdl))
    return sorted(scores.items(), key=lambda x: -x[1])[:k]


def single_search(index, docs, avgdl, terms, k):
    scores = {}
    for t in terms:
        if t in index:
            idf, plist = index[t]
            for d, tf in plist:
                dl = docs[d][0]
                scores[d] = scores.get(d, 0.0) + idf * tf * (K1 + 1) / (tf + K1 * (1 - B + B * dl / avgdl))
    return sorted(scores.items(), key=lambda x: -x[1])[:k]


def cmd_shards():
    index, docs, avgdl = load()
    global_stats = ({t: v[0] for t, v in index.items()}, avgdl)
    k = 10
    for n in (4, 16):
        shards = build_shards(index, docs, n)
        sizes = [s["n"] for s in shards]
        print(f"\n== {n} document shards: {min(sizes)}-{max(sizes)} documents each")
        same_local = same_global = 0
        for q in QUERIES:
            terms = analyse(q)
            truth = [d for d, _ in single_search(index, docs, avgdl, terms, k)]
            # fan out: every shard returns its own top k, the aggregator merges them
            local = sorted((h for s in shards for h in shard_search(s, terms, docs, k)), key=lambda x: -x[1])[:k]
            glob = sorted((h for s in shards for h in shard_search(s, terms, docs, k, global_stats)), key=lambda x: -x[1])[:k]
            ol = len(set(truth) & {d for d, _ in local})
            og = len(set(truth) & {d for d, _ in glob})
            same_local += [d for d, _ in local] == truth
            same_global += [d for d, _ in glob] == truth
            print(f"  {q:<30} top-{k} overlap with one index:  local stats {ol:>2}/{k}   global stats {og:>2}/{k}")
        print(f"  identical ranking: local stats {same_local}/{len(QUERIES)} queries, global stats {same_global}/{len(QUERIES)}")
    print("""
Reading it
- Each shard returns only its own top 10; the aggregator merges them. The global
  top 10 is always among those, so merging loses nothing.
- With LOCAL statistics each shard judges word rarity (idf) and typical document
  length from its own documents only, so scores differ between shards and the
  merged ranking drifts. Fewer documents per shard, more drift.
- With GLOBAL statistics (shared across shards, as Elasticsearch's
  "dfs_query_then_fetch" does) the sharded answer matches the single index. At
  large scale random sharding makes local statistics close enough, so most
  engines accept small drift for one fewer round trip.""")


# --- Part 2: tail latency (simulation) -----------------------------------------------
def shard_latency(rng):
    """Assumed per-shard response time in ms: usually about 5 ms, but 1% of requests
    stall for 50-150 ms (garbage collection, a slow disk, a busy neighbour)."""
    base = rng.lognormvariate(1.6, 0.25)              # median about 5 ms
    return base + (rng.uniform(50, 150) if rng.random() < 0.01 else 0.0)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))]


def cmd_tail():
    rng = random.Random(42)
    n_queries = 20000
    sample = [shard_latency(rng) for _ in range(100000)]
    hedge_after = pct(sample, 0.95)
    print(f"Per-shard model: median {pct(sample, .5):.1f} ms, p99 {pct(sample, .99):.1f} ms; "
          f"hedge after p95 = {hedge_after:.1f} ms\n")
    print(f"{'shards':>6} {'p50 ms':>8} {'p99 ms':>8} {'queries hitting a stall':>24}"
          f"   {'hedged p99':>10} {'extra requests':>15}")
    for n in (1, 4, 16, 40):
        plain, hedged, stalls, extra = [], [], 0, 0
        for _ in range(n_queries):
            lat = [shard_latency(rng) for _ in range(n)]
            plain.append(max(lat))
            stalls += max(lat) > 40
            h = []
            for first in lat:
                if first > hedge_after:          # no answer yet: ask another replica
                    extra += 1
                    first = min(first, hedge_after + shard_latency(rng))
                h.append(first)
            hedged.append(max(h))
        print(f"{n:>6} {pct(plain, .5):>8.1f} {pct(plain, .99):>8.1f} {stalls / n_queries:>23.1%}"
              f"   {pct(hedged, .99):>10.1f} {extra / (n * n_queries):>14.1%}")
    print("""
Reading it
- A query waits for its slowest shard. With 1 shard, 1% of queries hit a stall;
  with 40 shards about a third do: rare slowness becomes common slowness.
- Hedging: if a shard has not answered by its p95, send the same request to
  another replica and take whichever answers first. It costs about 5% extra
  requests and removes almost all of the tail.
- This is a model with assumed numbers, not a measurement: the shape is the
  point, and it matches what large search systems report.""")


if __name__ == "__main__":
    {"shards": cmd_shards, "tail": cmd_tail}.get(sys.argv[1] if len(sys.argv) > 1 else "",
                                                 lambda: print(__doc__))()
