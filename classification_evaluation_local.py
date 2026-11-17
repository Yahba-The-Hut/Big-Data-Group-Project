"""Member 4: batch classification and manual-label evaluation.

Loads the saved Part 3 PipelineModel (no retraining), classifies the collected
Hacker News messages, exports predictions with confidence, samples 50 messages
for hand labeling, and computes accuracy on the labeled sample.

Run from the project root:
    python classification_evaluation_local.py

Optional arguments:
    --messages   path to collected messages (default: data/messages.jsonl)
    --model-dir  path to saved model         (default: models/ag_news_pipeline)
    --output-dir where outputs are written    (default: classification_outputs)
"""

import argparse
import json
import os
from itertools import chain

import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_max, col, create_map, length, lit, rand, trim

LABEL_MAP = {0: "World", 1: "Sports", 2: "Business", 3: "Sci/Tech"}
VALID_LABELS = ["World", "Sports", "Business", "Sci/Tech", "None of these"]
SAMPLE_SEED = 4371

p = argparse.ArgumentParser()
p.add_argument("--messages", default="data/messages.jsonl")
p.add_argument("--model-dir", default="models/ag_news_pipeline")
p.add_argument("--output-dir", default="classification_outputs")
a = p.parse_args()

os.makedirs(a.output_dir, exist_ok=True)
predictions_dir = os.path.join(a.output_dir, "batch_predictions")
sample_file = os.path.join(a.output_dir, "manual_labels_50.csv")
labeled_file = os.path.join(a.output_dir, "manually_labelled_data.csv")
results_file = os.path.join(a.output_dir, "evaluation_results.csv")
summary_file = os.path.join(a.output_dir, "accuracy_summary.json")

spark = (
    SparkSession.builder
    .appName("GP1-Member4-BatchClassification")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------
# 1. Load and clean messages
# ---------------------------------------------------------------
raw = spark.read.json(a.messages).select(
    col("id").cast("string").alias("id"),
    col("text").cast("string").alias("text"),
)
raw_count = raw.count()

clean = (
    raw.withColumn("text", trim(col("text")))
    .where(col("text").isNotNull() & (length(col("text")) > 0))
    .dropDuplicates(["text"])
)
clean_count = clean.count()

print(f"RAW COUNT: {raw_count}")
print(f"CLEAN COUNT: {clean_count} (removed {raw_count - clean_count} empty or duplicate)")

# ---------------------------------------------------------------
# 2. Predict with the saved model
# ---------------------------------------------------------------
model = PipelineModel.load(a.model_dir)

mapping_expr = create_map(*chain.from_iterable((lit(k), lit(v)) for k, v in LABEL_MAP.items()))

pred = (
    model.transform(clean)
    .withColumn("predicted_category", mapping_expr[col("prediction").cast("int")])
    .withColumn("confidence", array_max(vector_to_array(col("probability"))))
)

batch_results = pred.select("id", "text", "predicted_category", "confidence").cache()

print("\nPredicted class counts:")
batch_results.groupBy("predicted_category").count().orderBy(col("count").desc()).show()

print("Five example predictions per class:")
for label in LABEL_MAP.values():
    print(f"\n--- {label} ---")
    (batch_results.where(col("predicted_category") == label)
     .orderBy(col("confidence").desc())
     .select("text", "confidence")
     .show(5, truncate=85))

# escape='"' writes standard CSV quoting so the file opens correctly in Excel/pandas
(batch_results.coalesce(1).write.mode("overwrite")
 .option("header", True)
 .option("escape", '"')
 .csv(predictions_dir))
print(f"Predictions written to {predictions_dir}/ (one part-*.csv file)")

# ---------------------------------------------------------------
# 3. 50-message sample for hand labeling
# ---------------------------------------------------------------
# The sample is only created once. Re-running never overwrites a sample that
# has already been labeled.
if not os.path.exists(labeled_file):
    sample_pd = (
        batch_results.orderBy(rand(seed=SAMPLE_SEED)).limit(50).toPandas()
    )
    if len(sample_pd) != 50:
        raise ValueError("Need at least 50 cleaned messages.")
    sample_pd["manual_label"] = ""
    sample_pd.to_csv(sample_file, index=False)
    print(f"\nSaved 50 messages to {sample_file}.")
    print("Fill in manual_label for every row (World / Sports / Business / Sci/Tech / None of these),")
    print(f"save the result as {labeled_file}, then run this script again.")
    spark.stop()
    raise SystemExit(0)

# ---------------------------------------------------------------
# 4. Evaluate the labeled sample
# ---------------------------------------------------------------
# sep=None sniffs comma vs semicolon (Excel in some locales saves with ';'),
# utf-8-sig drops the hidden byte-order mark Excel adds, and column names are
# normalized so "Manual Label" or " manual_label " still match.
evaluation = pd.read_csv(labeled_file, sep=None, engine="python", encoding="utf-8-sig")
evaluation.columns = [str(c).strip().lower().replace(" ", "_") for c in evaluation.columns]

missing = {"text", "predicted_category", "manual_label"} - set(evaluation.columns)
if missing:
    raise ValueError(
        f"{labeled_file} is missing column(s) {sorted(missing)}. "
        f"Columns found: {list(evaluation.columns)}. "
        "The header row should be: id,text,predicted_category,confidence,manual_label"
    )

evaluation["manual_label"] = evaluation["manual_label"].fillna("").astype(str).str.strip()

bad = evaluation[~evaluation["manual_label"].isin(VALID_LABELS)]
if len(evaluation) != 50 or len(bad) > 0:
    raise ValueError(
        f"{labeled_file} must have 50 rows, each labeled one of {VALID_LABELS}. "
        f"Rows with missing or invalid labels: {len(bad)}"
    )

is_none = evaluation["manual_label"] == "None of these"
none_count = int(is_none.sum())
eligible = evaluation[~is_none]
if len(eligible) == 0:
    raise ValueError("No eligible (non-None) messages to evaluate.")

correct = int((eligible["manual_label"] == eligible["predicted_category"]).sum())
accuracy = correct / len(eligible) * 100

evaluation["correct"] = (evaluation["manual_label"] == evaluation["predicted_category"]).astype("boolean")
evaluation.loc[is_none, "correct"] = pd.NA
evaluation.to_csv(results_file, index=False)

summary = {
    "raw_messages": raw_count,
    "clean_messages": clean_count,
    "total_labeled": len(evaluation),
    "none_of_these": none_count,
    "eligible_messages": len(eligible),
    "correct_predictions": correct,
    "accuracy_percent": round(accuracy, 2),
}
with open(summary_file, "w") as f:
    json.dump(summary, f, indent=4)

print("\n=== Manual evaluation ===")
print(f"Labeled: {len(evaluation)} | None of these: {none_count} | Eligible: {len(eligible)}")
print(f"Accuracy: {correct}/{len(eligible)} = {accuracy:.2f}%")

wrong = evaluation[(evaluation["correct"] == False)]  # noqa: E712
print("\nMisclassified examples (for the report):")
for _, r in wrong.head(5).iterrows():
    print(f"  predicted {r['predicted_category']}, actually {r['manual_label']}: {r['text'][:90]}")

print(f"\nSaved {results_file} and {summary_file}")
spark.stop()