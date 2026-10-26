#!/usr/bin/env bash
# Collect a document corpus and land it in HDFS, for the search index.
# The "crawl": the README, NEWS and text files under /usr/share/doc on this host,
# packed into ONE JSON-lines file (one document per line), the way crawlers store
# their output, rather than as hundreds of small files.
# Run from the lab folder:  bash jobs/collect_docs.sh
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/docs

python3 - <<'EOF'
import json, os, re
out, n, size = "data/docs/corpus.jsonl", 0, 0
with open(out, "w", encoding="utf-8") as f:
    for root, _, files in os.walk("/usr/share/doc"):
        for name in sorted(files):
            if not re.match(r"(?i)(readme|news|changelog|faq|.*\.(txt|md))", name):
                continue
            path = os.path.join(root, name)
            try:
                if os.path.islink(path) or os.path.getsize(path) < 1024:
                    continue
                text = open(path, encoding="utf-8", errors="replace").read()
            except OSError:
                continue
            n += 1; size += len(text)
            f.write(json.dumps({"doc_id": n, "path": path, "text": text}) + "\n")
print(f"collected {n} documents, {size / 1e6:.1f} MB of text -> {out}")
EOF

docker compose exec -T edge bash -c '
hdfs dfs -mkdir -p /lake/raw/docs
hdfs dfs -put -f /input/docs/corpus.jsonl /lake/raw/docs/corpus.jsonl
hdfs dfs -ls /lake/raw/docs'
