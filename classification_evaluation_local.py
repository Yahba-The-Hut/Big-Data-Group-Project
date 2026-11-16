import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, length, lit, rand,
    create_map, array_max, monotonically_increasing_id
)
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.ml import PipelineModel

from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    ConfusionMatrixDisplay
)

from pyspark.sql import SparkSession
from pathlib import Path

BASE_DIR = Path(r"C:\\Users\\hanna\\Documents\\cs4371\\Big-Data-Group-Project-Phil")


os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = (
    r"C:\hadoop\bin" + os.pathsep + os.environ["PATH"]
)

spark = (
    SparkSession.builder
    .appName("BigDataGroupProject")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"
    )
    .getOrCreate()
)

# print(spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
# print("Spark version:", spark.version)


msgs = spark.read.json(f"{BASE_DIR}/data/messages.jsonl").select("id", "text")
# input_file = BASE_DIR / "data" / "messages.jsonl"

# print(BASE_DIR)
# print("Base directory exists:", os.path.isdir(BASE_DIR))
# print("Base directory writable:", os.access(BASE_DIR, os.W_OK))

MODEL_PATH = BASE_DIR / "models" / "ag_news_pipeline"
OUTPUT_DIR = BASE_DIR / "classification_outputs"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# print("Model exists:", os.path.exists(MODEL_PATH))
# msgs.show(truncate=False)
news_df = msgs

KAFKA_SERVERS = "YOUR_ACCESSIBLE_BROKER:9092"
KAFKA_TOPIC = "topic1"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages "
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"
    "pyspark-shell"
)


kafka_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_SERVERS
    )
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

print("Kafka source configured")

# news_df.printSchema()
# news_df.show(5, truncate=False)

# print("Total messages:", news_df.count())

#clean
SOURCE_TEXT_COL = "text"
SOURCE_ID_COL = "id"

if SOURCE_TEXT_COL not in news_df.columns:
    raise ValueError("Text column not found.")

if SOURCE_ID_COL in news_df.columns:
    clean_df = news_df.select(
        col(SOURCE_ID_COL).cast("string").alias("id"),
        trim(col(SOURCE_TEXT_COL).cast("string")).alias("text")
    )
else:
    clean_df = news_df.select(
        trim(col(SOURCE_TEXT_COL).cast("string")).alias("text")
    ).withColumn(
        "id",
        monotonically_increasing_id().cast("string")
    )

clean_df = (
    clean_df
    .filter(col("text").isNotNull())
    .filter(length(col("text")) > 0)
    .dropDuplicates(["text"])
)

print("Cleaned messages:", clean_df.count())

clean_df.show(5, truncate=False)

#model

model = PipelineModel.load(MODEL_PATH)

for i, stage in enumerate(model.stages):
    print(i, type(stage).__name__)

MODEL_TEXT_COL = model.stages[0].getInputCol()

print("\nModel expects:", MODEL_TEXT_COL)

if MODEL_TEXT_COL != "text":
    model_input_df = clean_df.withColumn(
        MODEL_TEXT_COL,
        col("text")
    )
else:
    model_input_df = clean_df


#real model

predictions_df = model.transform(news_df)

predictions_df.select(
    "text",
    "prediction",
    "probability"
).show(10, truncate=False)

from itertools import chain

label_map = {
    0: "World",
    1: "Sports",
    2: "Business",
    3: "Sci/Tech"
}

mapping_expr = create_map(
    *list(chain.from_iterable(
        (lit(k), lit(v))
        for k, v in label_map.items()
    ))
)

predictions_df = (
    predictions_df
    .withColumn(
        "predicted_category",
        mapping_expr[col("prediction").cast("int")]
    )
    .withColumn(
        "confidence",
        array_max(vector_to_array(col("probability")))
    )
)

batch_results = predictions_df.select(
    "id",
    "text",
    "predicted_category",
    "confidence"
)

batch_results.show(10, truncate=False)

batch_results.write.mode("overwrite").option(
    "header", True
).csv(f"{OUTPUT_DIR}/batch_predictions")

sample_pd = (
    batch_results
    .orderBy(rand(seed=4371))
    .limit(50)
    .toPandas()
)

if len(sample_pd) != 50:
    raise ValueError("Need at least 50 cleaned messages.")

sample_pd["manual_label"] = ""

LABEL_FILE = f"{OUTPUT_DIR}/manual_labels_50.csv"

sample_pd.to_csv(LABEL_FILE, index=False)

print("Saved 50 messages:", LABEL_FILE)
# print("\nManual Labelling will be done at this point")

evaluation = pd.read_csv(f"{OUTPUT_DIR}/manually_labelled_data.csv")

evaluation["manual_label"] = (
    evaluation["manual_label"].fillna("").str.strip()
)

valid_labels = [
    "World", "Sports", "Business",
    "Sci/Tech", "None of these"
]

if len(evaluation) != 50 or not evaluation[
    "manual_label"
].isin(valid_labels).all():
    raise ValueError("Complete all 50 manual labels first.")

eligible = evaluation[
    evaluation["manual_label"] != "None of these"
].copy()

eligible["correct"] = (
    eligible["manual_label"]
    == eligible["predicted_category"]
)

total = len(eligible)

if total == 0:
    raise ValueError("No eligible messages to evaluate.")

correct = int(eligible["correct"].sum())
accuracy = correct / total * 100

print("Total labeled:", len(evaluation))
print("Eligible:", total)
print("Correct:", correct)
print(f"Accuracy: {accuracy:.2f}%")

evaluation["correct"] = (
    evaluation["manual_label"]
    == evaluation["predicted_category"]
).astype("boolean")

evaluation.loc[
    evaluation["manual_label"] == "None of these",
    "correct"
] = pd.NA

evaluation.to_csv(
    f"{OUTPUT_DIR}/evaluation_results.csv",
    index=False
)

with open(
    f"{OUTPUT_DIR}/accuracy_summary.json", "w"
) as f:
    json.dump({
        "total_labeled": len(evaluation),
        "eligible_messages": total,
        "correct_predictions": correct,
        "accuracy_percent": round(accuracy, 2)
    }, f, indent=4)

print("Evaluation saved!")
