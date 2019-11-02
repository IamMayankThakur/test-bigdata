from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *

#Create A spark Session
spark = SparkSession.builder.appName("modeHashtags").getOrCreate()


#Creating A userSchema
spark_schema = StructType().add("Id", "string").add("Lang", "string").add("Date", "string").add("Source", "string").add("len", "integer").add("Likes", "integer").add("RTs", "string").add("Hashtags", "string").add("UserMentionNames", "string").add("UserMentionID", "integer").add("Name", "string").add("Place", "string").add("Followers", "integer").add("Friends", "integer")



df = spark.readStream.option("sep", ";").schema(spark_schema).csv("hdfs://localhost:9000/stream/")
#df=df.groupBy("Hashtags").count().select("Hashtags")
split_hashtags=df.select("Hashtags",explode(split("Hashtags",",")).alias("hash"))
hash_counts=split_hashtags.groupBy("hash").count()
hash_counts.registerTempTable("TABLE")
new=spark.sql("SELECT hash as Hashtags,count FROM TABLE order by count desc limit 5")
query = new.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(60)
query.stop()
#query1.awaitTermination(50)

#print(csvDF.select("Id"))

#print(csvDF.isStreaming)
#/home/satyam/Desktop/final/A-3/dataset
