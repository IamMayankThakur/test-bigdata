import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *

spark = SparkSession\
    .builder\
    .appName("TwitterAnalysis")\
    .getOrCreate()

inputPath = "hdfs://localhost:9000/stream"

schema = StructType().add("ID", "string").add("Lang", "string").add(
    "Date", "date").add("Source", "string").add("len", "integer").add("Likes", "string").add(
        "RTs", "integer").add("Hashtags", "string").add("UserMentionNames", "string").add("UserMentionID", "string").add(
            "name", "string").add("Place", "string").add("Followers", "double").add("Friends", "double")

dataStream = spark.readStream.csv(
    inputPath, sep=";", schema=schema)
popularity_count = dataStream.withColumn(
    "FRRatio", col("Followers")/col("Friends")).groupBy("name").agg(max(col("FRRatio"))).orderBy("max(FRRatio)", ascending=False).withColumnRenamed("max(FRRatio)","FRRatio").limit(1)

query = popularity_count\
    .writeStream\
    .format('console')\
    .outputMode("complete")\
    .start()

query.awaitTermination(60)
query.stop()
