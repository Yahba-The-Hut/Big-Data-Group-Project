# CS4371 Group Project 1

Hacker News text streamed through Kafka into two Spark Structured Streaming branches:

1. **Entity branch:** spaCy NER → running entity counts → Kafka `topic2` → Logstash → Elasticsearch → Kibana top-10 chart
2. **Classification branch:** Spark MLlib model trained on AG News → batch predictions on collected messages + live class counts

| Part | Owner | Files |
|---|---|---|
| 1. Data collection + Kafka | Member 1 | `producer.py`, `setup_kafka.sh`, `validate.py`, `test_offline.py` |
| 2. NER + entity counts + Kibana | Member 2 | `streaming_ner.py`, `entity_counts.py`, `test_ner.py` |
| 3. MLlib model | Member 3 | `get_data.py`, `train_model.py`, `smoke_test.py`, `models/ag_news_pipeline/` |
| 4. Batch + live classification | Member 4 | `classification_evaluation_local.py`, `streaming_classifier.py`, `classification_outputs/` |

## Repository layout

```
producer.py, setup_kafka.sh, validate.py, test_offline.py    # Part 1
streaming_ner.py, entity_counts.py, test_ner.py              # Part 2
get_data.py, train_model.py, smoke_test.py                   # Part 3
classification_evaluation_local.py, streaming_classifier.py  # Part 4
data/messages.jsonl       # all collected messages (shared input for Parts 2 and 4)
data/checkpoint.json      # producer resume state
models/ag_news_pipeline/  # saved PipelineModel (Part 3 output, Part 4 input)
classification_outputs/   # Part 4 predictions, manual labels, accuracy
evidence/                 # screenshots and logs
HANDOFF_Members_2_and_4.md
```

## Shared environment

| Component | Version |
|---|---|
| Python | 3.12+ |
| Spark / PySpark | 4.3.0 |
| Java | 17 |
| Kafka | `apache/kafka` Docker image (4.4.0) |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0` |

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows cmd: .venv\Scripts\activate
pip install -r requirements.txt
pip install pyspark==4.3.0 spacy datasets pyarrow "pandas<3" scikit-learn
python -m spacy download en_core_web_sm
```

Anything that reads Kafka from Spark needs the connector package:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 <script>.py ...
```

### Windows (non-WSL) notes

Running Spark directly on Windows needs extra setup. WSL avoids all of it.

1. Download `winutils.exe` and `hadoop.dll` (newest `hadoop-3.x` folder from the `cdarlint/winutils` GitHub repo) into `C:\hadoop\bin`, and copy `hadoop.dll` into `C:\Windows\System32` (needs admin).
2. Set these in each new terminal, or permanently with `setx` followed by a full VS Code restart:

```cmd
set SPARK_HOME=<project folder>\.venv\Lib\site-packages\pyspark
set PYSPARK_PYTHON=python
set PYSPARK_DRIVER_PYTHON=python
set SPARK_LOCAL_HOSTNAME=localhost
set HADOOP_HOME=C:\hadoop
set PATH=%PATH%;C:\hadoop\bin
```

| Error | Fix |
|---|---|
| `Missing Python executable 'python3'` / `Failed to find Spark jars directory` | Set `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON`, `SPARK_HOME` |
| `Invalid Spark URL: spark://HeartbeatReceiver@<name>` | Computer name has an underscore: set `SPARK_LOCAL_HOSTNAME=localhost` |
| `Did not find winutils.exe` / `HADOOP_HOME ... unset` | Install `winutils.exe`, set `HADOOP_HOME` |
| `UnsatisfiedLinkError ... NativeIO$Windows.access0` | Copy `hadoop.dll` to `System32`, or add `--driver-java-options "-Djava.library.path=C:\hadoop\bin"` |

---

## Part 1: Data collection and Kafka (Member 1)

Collects Hacker News stories and comments (no API key needed), strips HTML from the text, and streams each item as JSON to Kafka `topic1`. Every message is also saved to `data/messages.jsonl` for batch use.

**Requirements:** Windows + WSL2 (Ubuntu) + Docker Desktop with WSL integration, Python 3 with `venv`, Kafka image `apache/kafka` (tag `4.4.0`).

### Setup

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
```

Or run `bash setup_kafka.sh` to start Kafka and create both topics.

### Run

```bash
python producer.py \
  --bootstrap-server localhost:9092 \
  --topic topic1 \
  --output data/messages.jsonl \
  --checkpoint data/checkpoint.json \
  --backfill 2500
```

It backfills the most recent `--backfill` item IDs, then keeps polling for new items. Stop with Ctrl+C. Rerunning resumes from `data/checkpoint.json`.

### Message format (team contract)

```json
{"id": "12345678", "text": "cleaned text to analyze", "source": "hackernews",
 "collected_at": "2026-11-09T20:00:00+00:00", "type": "story", "timestamp": 1794300000}
```

Kafka key = item id as a string. `id`, `text`, `source`, `collected_at` are the contract. `type` (`story`/`comment`) and `timestamp` (original HN time) are extras. Use `text` for all analysis.

### Verify

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic topic1 --from-beginning --max-messages 5
wc -l data/messages.jsonl
python validate.py data/messages.jsonl
python test_offline.py          # parsing/cleaning checks, no Kafka or network
```

**Result:** 4,354 messages collected in `data/messages.jsonl`. Evidence: `evidence/01_kafka_running.png` to `06_topic_listing.png`.

### Notes

- Duplicates are possible after a crash (Kafka ack, file write, and checkpoint are not one transaction). Deduplicate on `id` downstream.
- Don't delete the checkpoint during normal use.
- `localhost:9092` only works on the machine running Kafka. Teammates on other computers should use `data/messages.jsonl`.
- Run any console-producer smoke test on a fresh topic so test text doesn't mix into the dataset.
- If this folder is synced by OneDrive, pause syncing while the producer runs.

See `HANDOFF_Members_2_and_4.md` for the handoff details.

---

## Part 2: Named entities, counts, Kibana (Member 2)

`streaming_ner.py` reads `topic1` with Spark Structured Streaming, extracts PERSON, ORG, and GPE entities with spaCy (`en_core_web_sm`), and publishes `{"entity", "count", "batch_id"}` JSON to `topic2` every 10 seconds.

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 streaming_ner.py
```

Check output:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic topic2 --from-beginning --max-messages 5
```

Offline helpers (read `data/messages.jsonl`, no Kafka needed):

- `python test_ner.py`: quick spaCy + Spark sanity check
- `python entity_counts.py`: prints the top 20 entities across the whole file

<!-- Member 2: add logstash.conf, Elasticsearch/Kibana setup, and the top-10 chart screenshots here. -->

---

## Part 3: MLlib text classifier (Member 3)

Trains one Spark MLlib `PipelineModel` on the AG News training set and saves it to `models/ag_news_pipeline`. Part 4 loads this exact model for both batch and streaming. **Do not retrain it.**

### Pipeline

`RegexTokenizer` (pattern `\W+`, lowercase) → `StopWordsRemover` → `HashingTF` (2^16 features) → `IDF` → `LogisticRegression` (multinomial, `maxIter=30`, `regParam=0.1`)

### Label map

| Label | Class |
|---|---|
| 0 | World |
| 1 | Sports |
| 2 | Business |
| 3 | Sci/Tech |

### Run

```bash
mkdir -p data models evidence
python get_data.py                 # downloads AG News train split to data/ag_news_train.parquet
spark-submit --driver-memory 4g train_model.py \
  --train-parquet ./data/ag_news_train.parquet \
  --model-out ./models/ag_news_pipeline
spark-submit smoke_test.py         # loads the model, predicts 4 sample sentences
```

The Parquet file is not committed. Rerun `get_data.py` to recreate it. Training overwrites `models/ag_news_pipeline` if it exists.

Trained with PySpark 4.3.0, Java 17.0.12, on Windows 11 (see Windows notes above).

### Results

- **Training rows:** 120,000 (`evidence/train_log.txt`, line starting `AG NEWS COUNT`)
- **Class distribution:** 4 classes, 30,000 each (exact table in the training log)
- **Smoke test:** all 4 sample sentences classified correctly (`evidence/Screenshots_Part3.pdf`)

| Text | Prediction | Top probability |
|---|---|---|
| The team won the championship | 1 (Sports) | 0.450 |
| Stocks fell after the earnings report | 2 (Business) | 0.421 |
| New smartphone chip announced by Apple | 3 (Sci/Tech) | 0.490 |
| Leaders meet for peace talks in Geneva | 0 (World) | 0.477 |

Top-class probabilities are moderate (about 0.42 to 0.49) because `regParam=0.1` regularizes strongly and flattens the distribution. Predictions are still correct, and the batch `confidence` column looks similar.

---

## Part 4: Batch and live classification (Member 4)

Uses the saved Part 3 model with no retraining.

### Batch classification and manual evaluation

`classification_evaluation_local.py` reads `data/messages.jsonl`, removes empty and duplicate texts, applies `models/ag_news_pipeline`, maps predictions to class names, and writes predictions with a `confidence` column (max class probability). It then samples 50 messages (`seed=4371`) for hand labeling (World / Sports / Business / Sci/Tech / None of these) and computes accuracy on the non-None rows.

```bash
python classification_evaluation_local.py
```

Outputs in `classification_outputs/`:

| File | Contents |
|---|---|
| `part-00000-*.csv` | Predictions for every message: `id`, `text`, `predicted_category`, `confidence` |
| `manual_labels_50.csv` | The 50 sampled messages before labeling |
| `manually_labelled_data.csv` | The same 50 with `manual_label` filled in |
| `evaluation_results.csv` | Labels side by side with a `correct` column |
| `accuracy_summary.json` | Totals and accuracy |

**Current result:** 27 / 50 correct = 54.0% accuracy, 0 marked None of these. Predicted class mix across all messages is heavily Sci/Tech, which fits Hacker News content.

### Live classification (second streaming query)

`streaming_classifier.py` reads `topic1`, runs each message through the saved model, and prints cumulative counts per predicted class (0 to 3) to the console every 30 seconds. It also prints incoming messages as a second console query.

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 streaming_classifier.py \
  --model-dir ./models/ag_news_pipeline \
  --checkpoint ./checkpoints/classifier
```

Use a checkpoint path separate from Part 2's. Deleting it replays `topic1` from the beginning.

---

## Full run order

1. Start Kafka and confirm `topic1` and `topic2` exist
2. Start Elasticsearch, Kibana, then Logstash
3. Start `streaming_ner.py` (Part 2)
4. Start `streaming_classifier.py` (Part 4)
5. Start `producer.py` (Part 1)
6. Watch `topic2` and the Kibana chart update
7. Run `classification_evaluation_local.py` for batch predictions and accuracy

## Not committed

`.venv/`, `data/*.parquet`, `checkpoints/`, `spark-warehouse/`, local env scripts, and any credentials or API keys.