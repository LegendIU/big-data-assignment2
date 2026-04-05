import sys
import re
from collections import Counter

TOKEN_RE = re.compile(r"[a-z0-9]+")

for raw in sys.stdin:
    raw = raw.rstrip("\n")
    if not raw:
        continue

    parts = raw.split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, doc_title, doc_text = parts
    tokens = TOKEN_RE.findall(doc_text.lower())

    if not tokens:
        continue

    doc_len = len(tokens)
    freqs = Counter(tokens)

    # метаданные документа
    print(f"__DOC__::{doc_id}\t{doc_id}\t{doc_title}\t{doc_len}")

    # глобальная статистика
    print("__STAT__::N\t1")
    print(f"__STAT__::TOTAL_DOC_LEN\t{doc_len}")

    # term -> document postings
    for term, tf in freqs.items():
        print(f"{term}\t{doc_id}\t{doc_title}\t{tf}\t{doc_len}")