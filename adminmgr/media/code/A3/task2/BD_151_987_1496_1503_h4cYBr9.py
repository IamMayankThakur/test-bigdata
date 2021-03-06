from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,desc,sum
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("StructuredStreaming") \
    .getOrCreate()

inputpath="hdfs://localhost:9000/stream/"
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
                      StructField("Followers", IntegerType(), True),
                      StructField("Friends",IntegerType(), True)])
lines = spark \
    .readStream \
    .schema(schema) \
    .option("sep", ";") \
    .csv(inputpath)

inputDF = lines.withColumn("FRRatio",lines.Followers/lines.Friends)
inputDF = inputDF.groupBy("name").agg(sum("FRRatio").alias("FRRatio"))
inputDF2 = inputDF.sort(desc("FRRatio")).limit(1)
query=inputDF2.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(100)
query.stop()


