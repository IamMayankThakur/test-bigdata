from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import csv
from pyspark.sql.functions import col,split,explode
from pyspark.sql.types import StructType
session = SparkSession.builder.appName("bigData").getOrCreate()

Schema = StructType()\
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
df = session.readStream.option("sep", ";").schema(Schema).csv("/stream")
df.createOrReplaceTempView("bigData")

q = session.sql("select name,max(followers/friends) as FRRatio from bigData group by name order by FRRatio desc limit 1")
query = q.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(100)
query.stop()
