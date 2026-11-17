"""Member 2: streaming named-entity counts.

Reads Hacker News messages from Kafka topic1, extracts named entities with
spaCy, keeps RUNNING (cumulative) counts per entity across the whole stream,
and publishes the full current count table to Kafka topic2 every trigger:

    key   = entity (lowercased)
    value = {"entity": "microsoft", "count": 42, "batch_id": 5}

Logstash indexes topic2 into Elasticsearch using the entity as the document
id, so each new count replaces the previous one (see logstash.conf).

Run (needs the Spark Kafka connector):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
        streaming_ner.py --checkpoint ./checkpoints/entities
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

ENTITY_LABELS = ("PERSON", "ORG", "GPE")

p = argparse.ArgumentParser()
p.add_argument("--bootstrap", default="localhost:9092")
p.add_argument("--input-topic", default="topic1")
p.add_argument("--output-topic", default="topic2")
p.add_argument("--checkpoint", default="./checkpoints/entities")
p.add_argument("--trigger", default="30 seconds")
a = p.parse_args()

# Load spaCy lazily, once per Python worker
_nlp = None


def extract_entities(text):
    global _nlp
    if not text:
        return []
    if _nlp is None:
        import spacy
        _nlp = spacy.load("en_core_web_sm")
    doc = _nlp(text)
    return [
        ent.text.strip().lower()
        for ent in doc.ents
        if ent.label_ in ENTITY_LABELS and ent.text.strip()
    ]


ner_udf = F.udf(extract_entities, ArrayType(StringType()))

spark = SparkSession.builder.appName("GP1-EntityCounts").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", a.bootstrap)
    .option("subscribe", a.input_topic)
    .option("startingOffsets", "earliest")
    .load()
)

texts = raw.select(
    F.get_json_object(F.col("value").cast("string"), "$.text").alias("text")
).where(F.col("text").isNotNull() & (F.length(F.trim("text")) > 0))

# Stateful aggregation: Spark keeps the running count for every entity
# across all micro-batches (stored in the checkpoint).
counts = (
    texts.select(F.explode(ner_udf("text")).alias("entity"))
    .groupBy("entity")
    .count()
)


def publish(batch_df, batch_id):
    # In complete mode, batch_df is the FULL cumulative count table.
    n = batch_df.count()
    if n == 0:
        return
    (
        batch_df.select(
            F.col("entity").cast("string").alias("key"),
            F.to_json(
                F.struct(
                    F.col("entity"),
                    F.col("count"),
                    F.lit(batch_id).alias("batch_id"),
                )
            ).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", a.bootstrap)
        .option("topic", a.output_topic)
        .save()
    )
    print(f"batch {batch_id}: published cumulative counts for {n} entities", flush=True)
    batch_df.orderBy(F.desc("count")).show(10, truncate=False)


query = (
    counts.writeStream.outputMode("complete")
    .foreachBatch(publish)
    .option("checkpointLocation", a.checkpoint)
    .trigger(processingTime=a.trigger)
    .start()
)

print(f"Streaming NER started: {a.input_topic} -> running entity counts -> {a.output_topic}")
query.awaitTermination()