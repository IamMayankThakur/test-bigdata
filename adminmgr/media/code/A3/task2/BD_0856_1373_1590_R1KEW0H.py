from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,sum
from pyspark.sql.functions import split,desc
import pyspark.sql.functions as f
from pyspark.sql.types import *

spark = SparkSession \
	.builder \
	.appName("StructuredNetworkWordCount") \
	.getOrCreate()
userSchema = StructType().add("Id", "integer").add("Lang", "string")\
		.add("Date", "string").add("Source", "string").add("len", "integer")\
		.add("Likes", "integer").add("RTs", "integer").add("Hashtags", "string")\
		.add("UserMentionNames", "string").add("UserMentionID", "string")\
		.add("Name", "string").add("Place", "string").add("Followers", "integer")\
		.add("Friends","integer")
lines = spark \
	.readStream \
	.schema(userSchema) \
	.option("sep", ";") \
	.csv("hdfs://localhost:9000/stream/")

lines.createOrReplaceTempView("table")
r1 =  spark.sql("SELECT Name AS name ,Followers/Friends AS ratio from table")
r  = r1.groupBy(r1.name).agg(sum("ratio").alias("FRRatio"))


query = r.orderby(desc("FRRatio")).select("name","FRRatio").limit(1)\
	.writeStream \
	.queryName("name") \
	.format("console") \
	.outputMode("complete")\
	.start()
query.awaitTermination(100)
query.stop()


