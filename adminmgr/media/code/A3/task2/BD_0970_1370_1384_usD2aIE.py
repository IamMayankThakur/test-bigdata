from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split,desc
import pyspark.sql.functions as f
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

lines.createOrReplaceTempView("table1")
df2 =  spark.sql("SELECT Name AS name ,Followers/Friends AS FRRatio from table1")
df3 = df2.groupBy(df2.name).agg(f.sum("FRRatio").alias("FRRatio"))
df4 = df3.sort(desc("FRRatio")).limit(1)

df5 =df4.select("name","FRRatio")
query = df5\
    .writeStream \
    .queryName("name") \
    .format("console") \
    .outputMode("complete")\
    .start()
query.awaitTermination(100)
query.stop()
    






