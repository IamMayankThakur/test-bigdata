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

rows = session.readStream.option("sep", ";").option("header", "false").schema(Schema).csv("/stream")
rows.createOrReplaceTempView("bigData")
output = session.sql("select hashtags as Hashtags,count(Hashtags) as count from bigData group by Hashtags order by count desc limit 5")

q = output.writeStream.outputMode("complete").format("console").start()
q.awaitTermination(100)
q.stop()
