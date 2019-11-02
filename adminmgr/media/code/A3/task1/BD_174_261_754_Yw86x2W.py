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
            "Name", "string").add("Place", "string").add("Followers", "double").add("Friends", "double")

dataStream = spark.readStream.csv(inputPath, sep=";", schema=schema)
expandRows = dataStream.withColumn("Hashtags", explode(split(dataStream.Hashtags, ",")))
countHashtags = expandRows.groupBy("Hashtags").count().orderBy("count", ascending=False).limit(5)

query = countHashtags\
    .writeStream\
    .format('console')\
    .outputMode("complete")\
    .start()

query.awaitTermination(60)
query.stop()
