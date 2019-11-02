from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,desc,max
from pyspark.sql.types import *
from pyspark.sql.types import StringType, StructType, StructField
spark = SparkSession \
    .builder \
    .appName("StructuredStreaming") \
    .getOrCreate()

inputpath="./stream/"
schema = StructType([ StructField("ID", StringType(), True),
                      StructField("Lang", StringType(), True),
                      StructField("Date", StringType(), True),
                      StructField("Source", StringType(), True),
                      StructField("Len", StringType(), True),
                      StructField("Likes", StringType(), True),
                      StructField("RTs", StringType(), True),
                      StructField("Hashtags", StringType(), True),
                      StructField("UserMentionNames", StringType(), True),
                      StructField("UserMentionID", StringType(), True),
                      StructField("name", StringType(), True),
                      StructField("Place", StringType(), True),
                      StructField("Followers", StringType(), True),
                      StructField("Friends", StringType(), True)])
lines = spark \
    .readStream \
    .schema(schema) \
    .option("sep", ";") \
    .csv(inputpath)

inputDF = lines.withColumn("FRRatio",lines.Followers/lines.Friends)
inputDF = inputDF.groupBy("name").agg(max("FRRatio").alias("FRRatio")).sort(desc("FRRatio")).select("name","FRRatio")
query=inputDF.writeStream.outputMode("complete").option("numRows",1).format("console").start()
query.awaitTermination(60)
query.stop()


