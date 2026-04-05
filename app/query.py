import math
import re
import sys
from cassandra.cluster import Cluster

KEYSPACE = "search_engine"

STOPWORDS = {
    "a", "an", "the", "of", "in", "on", "at", "to", "for", "and", "or", "is",
    "are", "was", "were", "be", "been", "by", "with", "as", "from"
}

def tokenize(text: str):
    return re.findall(r"[a-z0-9]+", text.lower())

def bm25_idf(N, df):
    return math.log((N - df + 0.5) / (df + 0.5) + 1.0)

def main():
    raw_query = " ".join(sys.argv[1:]).strip()

    if not raw_query:
        print("empty query")
        return

    raw_terms = tokenize(raw_query)
    if not raw_terms:
        print("no valid terms")
        return

    terms = [t for t in raw_terms if t not in STOPWORDS]
    if not terms:
        terms = raw_terms

    cluster = Cluster(["cassandra-server"])
    session = cluster.connect(KEYSPACE)

    meta_rows = list(session.execute("SELECT key, value FROM meta"))
    meta = {row.key: row.value for row in meta_rows}

    N = int(meta.get("doc_count", 0))
    avg_dl = float(meta.get("avg_doc_len", 0.0))

    if N == 0 or avg_dl == 0:
        print("metadata missing")
        cluster.shutdown()
        return

    get_vocab = session.prepare("SELECT df FROM vocab WHERE term=?")
    get_postings = session.prepare(
        "SELECT doc_id, title, tf, doc_len, df FROM postings WHERE term=?"
    )

    k1 = 1.2
    b = 0.75

    scores = {}
    titles = {}

    for term in terms:
        vocab_row = session.execute(get_vocab, (term,)).one()
        if not vocab_row:
            continue

        df = int(vocab_row.df)
        idf = bm25_idf(N, df)

        rows = session.execute(get_postings, (term,))
        for row in rows:
            tf = float(row.tf)
            dl = float(row.doc_len)

            denom = tf + k1 * (1.0 - b + b * (dl / avg_dl))
            score = idf * ((tf * (k1 + 1.0)) / denom)

            doc_id = int(row.doc_id)
            scores[doc_id] = scores.get(doc_id, 0.0) + score
            titles[doc_id] = row.title

    # небольшой бонус за совпадения терминов в title
    for doc_id in list(scores.keys()):
        title_tokens = set(tokenize(titles[doc_id].replace("_", " ")))
        overlap = sum(1 for t in set(terms) if t in title_tokens)
        scores[doc_id] += 1.5 * overlap

    cluster.shutdown()

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]

    if not ranked:
        print("no results")
        return

    for i, (doc_id, score) in enumerate(ranked, 1):
        print(f"{i}\t{score:.4f}\t{doc_id}\t{titles[doc_id]}")

if __name__ == "__main__":
    main()