"""CS4371 GP1 - Member 1: Hacker News -> Kafka producer."""
import argparse
import html
import json
import re
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from pathlib import Path

import requests

BASE_URL = "https://hacker-news.firebaseio.com/v0"


class HTMLCleaner(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)

    def handle_starttag(self, tag, attrs):
        if tag in ("p", "br"):
            self.parts.append(" ")


def clean_text(value):
    cleaner = HTMLCleaner()
    cleaner.feed(value or "")
    text = html.unescape(" ".join(cleaner.parts))
    return re.sub(r"\s+", " ", text).strip()


def get_json(url):
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError):
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def parse_item(item_id, item):
    """Turn a raw HN item dict into a message record (or None)."""
    if not item or item.get("deleted") or item.get("dead"):
        return None
    kind = item.get("type")
    if kind == "story":
        raw = " ".join([item.get("title", ""), item.get("text", "")])
    elif kind == "comment":
        raw = item.get("text", "")
    else:
        return None
    text = clean_text(raw)
    if not text:
        return None
    # Team contract: id (string), text, source, collected_at (ISO-8601 UTC).
    # "type" and "timestamp" (original HN time) are extra, harmless fields.
    return {
        "id": str(item_id),
        "text": text,
        "source": "hackernews",
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "type": kind,
        "timestamp": item.get("time"),
    }


def fetch_item(item_id):
    return parse_item(item_id, get_json(f"{BASE_URL}/item/{item_id}.json"))


def main():
    from kafka import KafkaProducer  # imported here so tests don't need Kafka

    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="topic1")
    parser.add_argument("--output", default="data/messages.jsonl")
    parser.add_argument("--checkpoint", default="data/checkpoint.json")
    parser.add_argument("--backfill", type=int, default=2500)
    parser.add_argument("--interval", type=int, default=15)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    output = Path(args.output)
    checkpoint = Path(args.checkpoint)
    output.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.parent.mkdir(parents=True, exist_ok=True)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server.split(","),
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda x: str(x).encode("utf-8"),
        acks="all",
        retries=5,
    )

    if checkpoint.exists():
        next_id = json.loads(checkpoint.read_text())["next_id"]
        print(f"Resuming from item {next_id}")
    else:
        latest = get_json(f"{BASE_URL}/maxitem.json")
        next_id = max(1, latest - args.backfill + 1)
        print(f"Starting backfill at item {next_id}")

    total = 0
    try:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            while True:
                latest = get_json(f"{BASE_URL}/maxitem.json")
                if next_id > latest:
                    print("No new items; waiting...")
                    time.sleep(args.interval)
                    continue

                end_id = min(latest, next_id + args.batch_size - 1)
                # A failed request raises here and stops the batch, so items
                # are never silently skipped.
                records = list(pool.map(fetch_item, range(next_id, end_id + 1)))
                valid = [r for r in records if r is not None]

                futures = [producer.send(args.topic, key=r["id"], value=r) for r in valid]
                for future in futures:
                    future.get(timeout=30)  # wait for Kafka ack

                with output.open("a", encoding="utf-8") as f:
                    for r in valid:
                        f.write(json.dumps(r, ensure_ascii=False) + "\n")

                next_id = end_id + 1
                checkpoint.write_text(json.dumps({"next_id": next_id}))
                total += len(valid)
                print(
                    f"Processed through ID {end_id} | "
                    f"Batch: {len(valid)} messages | This run: {total}"
                )
                time.sleep(1)
    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
