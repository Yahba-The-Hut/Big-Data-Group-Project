
import spacy
from pyspark.sql import SparkSession

# Load spaCy's English model
nlp = spacy.load("en_core_web_sm")

# Start Spark
spark = (
    SparkSession.builder
    .appName("Member2NERTest")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read Member 1's messages
df = spark.read.json("data/messages.jsonl")

# Take 5 messages for a small test
messages = df.select("text").limit(5).collect()

print("\n=== NAMED ENTITY EXTRACTION ===")

for row in messages:
    text = row["text"]
    doc = nlp(text)

    print("\nMESSAGE:", text[:150])
    print("ENTITIES:")

    for ent in doc.ents:
        if ent.label_ in ["PERSON", "ORG", "GPE"]:
            print(f"  {ent.text} ({ent.label_})")

spark.stop()
