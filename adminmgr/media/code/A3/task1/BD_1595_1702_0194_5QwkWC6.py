from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import csv
from pyspark.sql.functions import col,split,explode
from pyspark.sql.types import StructType
sp = SparkSession.builder.appName("myapp").getOrCreate()

mySchema = StructType()\
.add("id", "integer")\
.add("lang", "string")\
.add("date", "string")\
.add("source", "string")\
.add("len", "integer")\
.add("likes", "integer")\
.add("RT", "integer")\
.add("hashtags", "string")\
.add("usernames", "string")\
.add("userid", "string")\
.add("name", "string")\
.add("place", "string")\
.add("followers", "integer")\
.add("friends", "integer")

rows = sp.readStream.option("sep", ";").option("header", "false").schema(mySchema).csv("/stream")
rows.createOrReplaceTempView("tweet_temp")
out = sp.sql("select hashtags as Hashtags,count(Hashtags) as count from tweet_temp group by Hashtags order by count desc limit 5")

query = out.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(100)
query.stop()
