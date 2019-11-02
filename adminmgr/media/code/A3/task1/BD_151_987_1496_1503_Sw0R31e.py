from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,desc
from pyspark.sql.types import *
from pyspark.sql.types import StringType, StructType, StructField
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
                      StructField("Followers", StringType(), True),
                      StructField("Friends", StringType(), True)])
lines = spark \
    .readStream \
    .schema(schema) \
    .option("sep", ";") \
    .csv(inputpath)

inputDF = lines.select(lines.Hashtags)
inputDF1 = inputDF.select(explode(   
       split(inputDF.Hashtags, ","))
   .alias("Hashtags")
)
inputDF2 = inputDF1.groupBy("Hashtags").count().sort(desc("count")).select("Hashtags","count").limit(5)
query=inputDF2.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(60)
query.stop()


