from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import desc
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
userSchema = StructType().add("Id", "integer").add("Lang", "string").add("Date", "string").add("Source", "string").add("len", "integer").add("Likes", "integer").add("RTs", "integer").add("Hashtags", "string").add("UserMentionNames", "string").add("UserMentionID", "string").add("Name", "string").add("Place", "string").add("Followers", "integer").add("Friends","integer")
lines = spark \
    .readStream \
    .schema(userSchema) \
    .option("sep", ";") \
    .csv("hdfs://localhost:9000/stream/")

df2 = lines.select(lines.Hashtags)
df3 = df2.select(explode(   
       split(df2.Hashtags, ","))
   .alias("Hashtags")
)
df4 = df3.groupBy("Hashtags").count()
df4 = df4.sort(desc("count"))
df4 = df4.select("Hashtags","count").limit(5)
query = df4\
    .writeStream \
    .outputMode("complete")\
    .format("console") \
    .start()
query.awaitTermination(100)
query.stop()


