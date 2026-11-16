
import spacy
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Load spaCy once per Python worker
_nlp = None

def extract_entities(text):
    global _nlp

    if not text:
        return []

    if _nlp is None:
        _nlp = spacy.load("en_core_web_sm")

    doc = _nlp(text)

    return [
        ent.text.strip()
        for ent in doc.ents
        if ent.label_ in ("PERSON", "ORG", "GPE")
        and ent.text.strip()
    ]


# Start Spark
spark = (
    SparkSession.builder
    .appName("Member2StreamingNER")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Kafka message schema
schema = StructType([
    StructField("id", StringType()),
    StructField("text", StringType()),
    StructField("source", StringType()),
    StructField("collected_at", StringType())
])

# Read streaming messages from topic1
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "topic1")
    .option("startingOffsets", "earliest")
    .load()
)

# Convert Kafka JSON messages into columns
messages = (
    kafka_df
    .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .filter(F.col("text").isNotNull())
)

# Extract named entities
ner_udf = F.udf(extract_entities, ArrayType(StringType()))

entities = (
    messages
    .withColumn("entity", F.explode(ner_udf(F.col("text"))))
)

# Process each micro-batch and publish entity counts
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    counts = batch_df.groupBy("entity").count()

    output = (
        counts
        .withColumn("batch_id", F.lit(batch_id))
        .select(
            F.col("entity").cast("string").alias("key"),
            F.to_json(
                F.struct("entity", "count", "batch_id")
            ).alias("value")
        )
    )

    (
        output.write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "topic2")
        .save()
    )

    print(f"Published entity counts for batch {batch_id}")


# Start streaming
query = (
    entities.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/member2_ner_checkpoint")
    .trigger(processingTime="10 seconds")
    .start()
)

print("Streaming NER pipeline started.")
print("Reading topic1 and publishing entity counts to topic2.")

query.awaitTermination()
