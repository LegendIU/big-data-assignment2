import sys

current_key = None
values = []

def flush(key, vals):
    if key is None:
        return

    if key.startswith("__DOC__::"):
        # vals: doc_id \t doc_title \t doc_len
        first = vals[0].split("\t", 2)
        if len(first) == 3:
            doc_id, doc_title, doc_len = first
            print(f"DOC\t{doc_id}\t{doc_title}\t{doc_len}")
        return

    if key == "__STAT__::N":
        total = sum(int(v) for v in vals if v.strip())
        print(f"STAT\tN\t{total}")
        return

    if key == "__STAT__::TOTAL_DOC_LEN":
        total = sum(int(v) for v in vals if v.strip())
        print(f"STAT\tTOTAL_DOC_LEN\t{total}")
        return

    # обычный term
    postings = []
    for v in vals:
        parts = v.split("\t", 3)
        if len(parts) == 4:
            doc_id, doc_title, tf, doc_len = parts
            postings.append((doc_id, doc_title, tf, doc_len))

    df = len(postings)
    for doc_id, doc_title, tf, doc_len in postings:
        print(f"TERM\t{key}\t{df}\t{doc_id}\t{doc_title}\t{tf}\t{doc_len}")

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 1)
    key = parts[0]
    value = parts[1] if len(parts) > 1 else ""

    if current_key is None:
        current_key = key
        values = [value]
    elif key == current_key:
        values.append(value)
    else:
        flush(current_key, values)
        current_key = key
        values = [value]

flush(current_key, values)