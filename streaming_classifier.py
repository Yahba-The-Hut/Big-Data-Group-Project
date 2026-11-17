
# streaming_classifier.py

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel

# Command-line arguments
p = argparse.ArgumentParser()
p.add_argument('--bootstrap', default='localhost:9092')
p.add_argument('--model-dir', required=True)
p.add_argument('--checkpoint', required=True)
a = p.parse_args()

# Create Spark session
spark = (
    SparkSession.builder
    .appName('GP1-LiveClassifier')
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Load saved trained model
model = PipelineModel.load(a.model_dir)

# Read Kafka topic1 continuously
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", a.bootstrap)
    .option("subscribe", "topic1")
    .option("startingOffsets", "earliest")
    .load()
)

# Extract id and text from JSON messages
msgs = raw.select(
    F.get_json_object(
        F.col("value").cast("string"), "$.id"
    ).alias("id"),

    F.get_json_object(
        F.col("value").cast("string"), "$.text"
    ).alias("text")
)

# Remove empty text messages
text = msgs.where(
    F.col("text").isNotNull() &
    (F.length(F.trim(F.col("text"))) > 0)
)

# -----------------------------------------
# QUERY 1: Display incoming Kafka messages
# -----------------------------------------

messages_query = (
    text.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        str(Path(a.checkpoint) / "messages")
    )
    .trigger(processingTime="30 seconds")
    .start()
)

# -----------------------------------------
# QUERY 2: Real-time classification
# -----------------------------------------

# Apply previously saved PipelineModel
pred = model.transform(text)

# Group predictions by numeric class
counts = (
    pred.groupBy("prediction")
    .count()
)

# Display cumulative counts every 30 seconds
classification_query = (
    counts.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        str(Path(a.checkpoint) / "classification")
    )
    .trigger(processingTime="30 seconds")
    .start()
)

print("Streaming classifier started!")
print("Reading messages from Kafka topic1...")
print("Displaying prediction counts every 30 seconds...")

# Keep both streaming queries running
spark.streams.awaitAnyTermination()
