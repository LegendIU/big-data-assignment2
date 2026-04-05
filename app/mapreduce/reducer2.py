import sys

current_term = None
current_df = None

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 1)
    term = parts[0]
    df = parts[1] if len(parts) > 1 else "0"

    if term != current_term:
        if current_term is not None:
            print(f"VOCAB\t{current_term}\t{current_df}")
        current_term = term
        current_df = df

if current_term is not None:
    print(f"VOCAB\t{current_term}\t{current_df}")