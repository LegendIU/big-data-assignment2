import sys

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) >= 3 and parts[0] == "TERM":
        term = parts[1]
        df = parts[2]
        print(f"{term}\t{df}")