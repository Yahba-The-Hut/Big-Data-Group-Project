# train_model.py
import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
p = argparse.ArgumentParser()
p.add_argument('--train-parquet', required=True)
p.add_argument('--model-out', required=True)
a = p.parse_args()
spark = SparkSession.builder.appName('GP1-TrainMLlib').getOrCreate()
df = spark.read.parquet(a.train_parquet).select('text','label')
df = df.where(F.col('text').isNotNull()).withColumn(
 'label',F.col('label').cast('double'))
print('AG NEWS COUNT',df.count()); df.groupBy('label').count().show()
token = RegexTokenizer(inputCol='text',outputCol='tokens',
 pattern='\W+',toLowercase=True)
stop = StopWordsRemover(inputCol='tokens',outputCol='filtered')
tf = HashingTF(inputCol='filtered',outputCol='tf',numFeatures=1<<16)
idf = IDF(inputCol='tf',outputCol='features')
clf = LogisticRegression(featuresCol='features', labelCol='label',
 maxIter=30,regParam=0.1,family='multinomial')
pipe = Pipeline(stages=[token,stop,tf,idf,clf])
model = pipe.fit(df)
model.write().overwrite().save(a.model_out)
print('MODEL SAVED:',a.model_out)
spark.stop()