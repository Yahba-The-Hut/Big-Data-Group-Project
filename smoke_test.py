from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GP1-SmokeTest").getOrCreate()
m = PipelineModel.load("./models/ag_news_pipeline")
t = m.transform(spark.createDataFrame(
    [("The team won the championship",),
     ("Stocks fell after the earnings report",),
     ("New smartphone chip announced by Apple",),
     ("Leaders meet for peace talks in Geneva",)], ["text"]))
t.select("text", "prediction", "probability").show(truncate=False)
spark.stop()