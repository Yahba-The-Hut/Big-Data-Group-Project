# Handoff from Member 1 → Members 2 and 4

Member 1 collects Hacker News stories and comments and publishes them to Kafka **`topic1`**. Everything is also saved to `data/messages.jsonl`.

## What you get
| File / place | Contents |
|---|---|
| Kafka `topic1` @ `localhost:9092` | One JSON message per HN item. Key = item id (string). |
| `data/messages.jsonl` | Same messages, one JSON object per line. Share this file with teammates on other computers. |
| `data/checkpoint.json` | Producer resume position (ignore it; don't edit or delete). |

> The data files appear only **after Member 1 runs the producer** (see README.md). Until then `data/` is empty — ask Member 1 to confirm `python validate.py` says `At least 1,000 collected: True`.

## Message schema (team contract)
```json
{"id": "12345678", "text": "cleaned text, HTML removed", "source": "hackernews",
 "collected_at": "2026-11-09T20:00:00+00:00", "type": "comment", "timestamp": 1794300000}
```
`id`, `text`, `source`, `collected_at` are guaranteed. `type` is `story` or `comment`; `timestamp` is the original HN time. **Analyze the `text` field.** The Kafka *value* is JSON bytes, so parse it; it is not a plain string.

## Important caveats
- **Duplicates are possible** (e.g. after a producer crash/restart). Dedupe on `id` and/or `text`. Report your raw and clean counts.
- `topic1` may also contain smoke-test lines that aren't JSON; Member 1 should start from a fresh topic. If `from_json`/`get_json_object` returns null, filter those rows out.
- **`localhost:9092` only works on the computer running Kafka.** On a different computer you can't reach it; use `data/messages.jsonl` instead (or run everything on one machine).
- Both streaming apps read `topic1` independently: give each its own `checkpointLocation`. Don't delete checkpoints casually (it replays data).
- Start Kafka first: `bash setup_kafka.sh` (WSL), then `docker ps` should show `kafka` Up.

## Member 2 (streaming NER → topic2)
Subscribe to `topic1` with `startingOffsets=earliest`, extract `$.text`:

```python
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "topic1").option("startingOffsets", "earliest").load())
msgs = raw.select(F.get_json_object(F.col("value").cast("string"), "$.text").alias("text"))
```
Write counts to `topic2` as `{"entity":..., "count":..., "batch_id":...}` (key = entity). The Spark Kafka package must match your Spark/Scala version, e.g. `--packages org.apache.spark:spark-sql-kafka-0-10_2.13:<your Spark version>`.
To show live growth in your timed Kibana screenshots (15/30/45/60 min), have Member 1 leave the producer running during that period (it polls for new HN items every ~15 s once caught up).

## Member 4 (batch + live classification)
Either read from Kafka:
```python
raw = (spark.read.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "topic1")
       .option("startingOffsets", "earliest").option("endingOffsets", "latest").load())
msgs = raw.select(F.get_json_object(F.col("value").cast("string"), "$.id").alias("id"),
                  F.get_json_object(F.col("value").cast("string"), "$.text").alias("text"))
```
or, easier for development (no Kafka needed):
```python
msgs = spark.read.json("data/messages.jsonl").select("id", "text")   # pass the path as an argument!
```
Then drop empty text and duplicates, and report pre/post counts (assignment §2.1 needs ≥ 1,000 streamed messages). Use the fixed seed from the team guide (`F.rand(seed=4371)`) for the 50 hand-labelled samples.
Note: HN text is tech-heavy, so expect many "None of these" and Sci/Tech-skewed predictions; that's good material for your summary.

## Questions / problems
Run `python validate.py data/messages.jsonl` for counts. Kafka issues: `docker logs --tail 100 kafka`.
