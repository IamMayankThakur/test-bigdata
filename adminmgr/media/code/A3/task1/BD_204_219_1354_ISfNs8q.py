from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,count
from pyspark.sql.types import StructType



spark=SparkSession \
	.builder \
	.appName("Task1_1") \
	.getOrCreate()

userSchema=StructType().add("ID","string").add("Lang","string").add("Date","string").add("Source","string").add("len","integer").add("likes","integer").add("RT","integer").add("Hash","string").add("UserN","string").add("UserID","string").add("Names","string").add("Place","string").add("Follow","integer").add("Friend","integer")
csvdf=spark \
	.readStream \
	.option("sep",";") \
	.schema(userSchema) \
	.csv("hdfs://localhost:9000/stream")

tags=csvdf.select("Hash")

words = tags.select(
   explode(
       split("Hash", ",")
   ).alias("Hashtags")
)

wordcount=words.groupby("Hashtags").count()

most=wordcount.select("Hashtags","count").orderBy("count",ascending=False).limit(5)


query=most.writeStream.outputMode("complete").format("console").start()

query.awaitTermination(60)
query.stop()
