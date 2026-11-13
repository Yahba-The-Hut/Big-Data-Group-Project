"""Check data/messages.jsonl meets the 1,000-message requirement."""
import json
import sys
from pathlib import Path

path = Path(sys.argv[1] if len(sys.argv) > 1 else "data/messages.jsonl")
records, bad = [], 0
for line in path.open(encoding="utf-8"):
    if not line.strip():
        continue
    try:
        records.append(json.loads(line))
    except json.JSONDecodeError:
        bad += 1

valid = [r for r in records
         if isinstance(r.get("text"), str) and r["text"].strip()
         and "id" in r and r.get("source")]
print("Total saved records:", len(records))
print("Unparseable lines:", bad)
print("Valid nonempty messages:", len(valid))
print("Unique item IDs:", len({r["id"] for r in valid}))
print("Unique message texts:", len({r["text"].strip() for r in valid}))
print("At least 1,000 collected:", len(valid) >= 1000)
