# CS4371 GP1 — Member 1: Data Collection

Collects Hacker News stories and comments (no API key needed), cleans the HTML out of the text, and streams each item as JSON to Apache Kafka `topic1`. Every message is also saved to `data/messages.jsonl` for batch classification.

## Requirements
- Windows + WSL2 (Ubuntu) + Docker Desktop with WSL integration enabled
- Python 3 (with `venv`)
- Kafka container image `apache/kafka` (guide used tag `4.4.0`; use whichever tag you pulled)

## Setup

```bash
# 1. Start Kafka (first time only)
docker pull apache/kafka:4.4.0
docker run -d --name kafka -p 9092:9092 apache/kafka:4.4.0
# Later sessions: docker start kafka   (do NOT re-run docker run)

# 2. Create topics
for t in topic1 topic2; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --topic $t --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
done
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# 3. Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python producer.py \
  --bootstrap-server localhost:9092 \
  --topic topic1 \
  --output data/messages.jsonl \
  --checkpoint data/checkpoint.json \
  --backfill 2500
```

It first backfills the most recent `--backfill` item IDs, then keeps polling for new items. Stop with Ctrl+C; rerunning resumes from `data/checkpoint.json`.

## Message format (shared with Members 2 and 4)

```json
{"id": "12345678", "text": "cleaned text to analyze", "source": "hackernews",
 "collected_at": "2026-11-09T20:00:00+00:00", "type": "story", "timestamp": 1794300000}
```

Kafka key = item id as a string. `id`, `text`, `source`, `collected_at` are the team contract; `type` (`story`/`comment`) and `timestamp` (original HN time) are extras. Use `text` for all analysis.

## Verify

```bash
# Messages arriving in Kafka
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic topic1 --from-beginning --max-messages 5

# Count / validate the saved file
wc -l data/messages.jsonl
python validate.py data/messages.jsonl
```

`python test_offline.py` checks the cleaning/parsing logic without Kafka or network.

## Notes
- Duplicates are possible after a crash (Kafka ack, file write, and checkpoint are not one transaction). Deduplicate on `id` downstream.
- Don't delete the checkpoint during normal use.
- `localhost:9092` only works on the machine running Kafka; teammates on other computers should use `data/messages.jsonl`.
- Do the console-producer smoke test before collecting, or use a fresh `topic1`, so test text doesn't mix into the dataset.

## Quick start (WSL/Ubuntu)
```bash
bash setup_kafka.sh            # starts Kafka, creates topic1 + topic2
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
python test_offline.py         # no network needed
python producer.py --backfill 5000
python validate.py             # in another terminal, once running a while
```
Teammates (Members 2 and 4): see `HANDOFF_Members_2_and_4.md`.
Tip: if this folder is synced by OneDrive, pause syncing while the producer runs, or run it from a non-synced folder and copy `data/` afterwards.
