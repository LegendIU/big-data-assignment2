from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import subprocess

KEYSPACE = "search_engine"

def run(cmd: str) -> str:
    return subprocess.check_output(cmd, shell=True, text=True)

def main():
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect()

    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(KEYSPACE)

    session.execute("DROP TABLE IF EXISTS vocab")
    session.execute("DROP TABLE IF EXISTS postings")
    session.execute("DROP TABLE IF EXISTS meta")
    session.execute("DROP TABLE IF EXISTS docs")

    session.execute("""
        CREATE TABLE vocab (
            term text PRIMARY KEY,
            df int
        )
    """)

    session.execute("""
        CREATE TABLE postings (
            term text,
            doc_id bigint,
            title text,
            tf int,
            doc_len int,
            df int,
            PRIMARY KEY ((term), doc_id)
        )
    """)

    session.execute("""
        CREATE TABLE docs (
            doc_id bigint PRIMARY KEY,
            title text,
            doc_len int
        )
    """)

    session.execute("""
        CREATE TABLE meta (
            key text PRIMARY KEY,
            value double
        )
    """)

    insert_vocab = session.prepare(
        "INSERT INTO vocab (term, df) VALUES (?, ?)"
    )
    insert_posting = session.prepare(
        "INSERT INTO postings (term, doc_id, title, tf, doc_len, df) VALUES (?, ?, ?, ?, ?, ?)"
    )
    insert_doc = session.prepare(
        "INSERT INTO docs (doc_id, title, doc_len) VALUES (?, ?, ?)"
    )
    insert_meta = session.prepare(
        "INSERT INTO meta (key, value) VALUES (?, ?)"
    )

    input_text = run("hdfs dfs -cat /input/data/part-*")
    docs_seen = {}
    total_doc_len = 0

    for line in input_text.splitlines():
        parts = line.rstrip("\n").split("\t", 2)
        if len(parts) != 3:
            continue
        doc_id, title, text = parts
        doc_len = len(text.split())
        docs_seen[int(doc_id)] = (title, doc_len)
        total_doc_len += doc_len

    doc_count = len(docs_seen)
    avg_doc_len = (total_doc_len / doc_count) if doc_count else 0.0

    session.execute(insert_meta, ("doc_count", float(doc_count)))
    session.execute(insert_meta, ("avg_doc_len", float(avg_doc_len)))

    print(f"doc_count = {doc_count}")
    print(f"avg_doc_len = {avg_doc_len:.4f}")

    batch = BatchStatement()
    n = 0
    for doc_id, (title, doc_len) in docs_seen.items():
        batch.add(insert_doc, (doc_id, title, doc_len))
        n += 1
        if n % 200 == 0:
            session.execute(batch)
            batch = BatchStatement()
    if len(batch) > 0:
        session.execute(batch)
    print(f"loaded docs rows: {n}")

    vocab_text = run("hdfs dfs -cat /indexer/vocab/part-*")
    batch = BatchStatement()
    n = 0
    for line in vocab_text.splitlines():
        parts = line.rstrip("\n").split("\t")
        if len(parts) != 3 or parts[0] != "VOCAB":
            continue
        _, term, df = parts
        batch.add(insert_vocab, (term, int(df)))
        n += 1
        if n % 200 == 0:
            session.execute(batch)
            batch = BatchStatement()
    if len(batch) > 0:
        session.execute(batch)
    print(f"loaded vocab rows: {n}")

    index_text = run("hdfs dfs -cat /indexer/index/part-*")
    batch = BatchStatement()
    n = 0
    for line in index_text.splitlines():
        parts = line.rstrip("\n").split("\t")
        if len(parts) != 7 or parts[0] != "TERM":
            continue
        _, term, df, doc_id, title, tf, doc_len = parts
        batch.add(
            insert_posting,
            (term, int(doc_id), title, int(tf), int(doc_len), int(df))
        )
        n += 1
        if n % 200 == 0:
            session.execute(batch)
            batch = BatchStatement()
            if n % 5000 == 0:
                print(f"loaded postings: {n}")
    if len(batch) > 0:
        session.execute(batch)

    print(f"loaded postings rows: {n}")
    cluster.shutdown()

if __name__ == "__main__":
    main()