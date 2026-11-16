
# streaming_classifier.py

import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel

# Command-line arguments
p = argparse.ArgumentParser()
p.add_argument('--bootstrap', default='localhost:9092')
p.add_argument('--model-dir', required=True)
p.add_argument('--checkpoint', required=True)
a = p.parse_args()

# Create Spark session
spark = SparkSession.builder \
    .appName('GP1-LiveClassifier') \
    .getOrCreate()

# Load saved trained model
model = PipelineModel.load(a.model_dir)

# Read Kafka topic1 continuously
raw = (
    spark.readStream.format("kafka")
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

# Apply previously saved PipelineModel
pred = model.transform(text)

# Group predictions by numeric class
counts = pred.groupBy("prediction").count()

# Display cumulative counts every 30 seconds
q = (
    counts.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", a.checkpoint)
    .trigger(processingTime="30 seconds")
    .start()
)

q.awaitTermination()
