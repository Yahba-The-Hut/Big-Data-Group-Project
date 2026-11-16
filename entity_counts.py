
import spacy
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# Load the NER model once per Python worker
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

spark = (
    SparkSession.builder
    .appName("Member2EntityCounts")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read Member 1's saved messages
df = spark.read.json("data/messages.jsonl")

# Register entity extraction function
ner_udf = F.udf(
    extract_entities,
    ArrayType(StringType())
)

# Extract entities from each message
entities = (
    df.select("id", "text")
    .filter(F.col("text").isNotNull())
    .withColumn("entity", F.explode(ner_udf(F.col("text"))))
)

# Count occurrences of each entity
counts = (
    entities.groupBy("entity")
    .count()
    .orderBy(F.desc("count"))
)

print("\n=== TOP 20 NAMED ENTITIES ===")
counts.show(20, truncate=False)

spark.stop()
