from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *

#Create A spark Session
spark = SparkSession.builder.appName("modeHashtags").getOrCreate()


#Creating A userSchema
spark_schema = StructType().add("Id", "string").add("Lang", "string").add("Date", "string").add("Source", "string").add("len", "integer").add("Likes", "integer").add("RTs", "string").add("hashs", "string").add("UserMentionNames", "string").add("UserMentionID", "integer").add("Name", "string").add("Place", "string").add('Followers', "integer").add('Friends', "integer")



df = spark.readStream.option("sep", ";").schema(spark_schema).csv("hdfs://localhost:9870/stream/")
df=df.withColumn("FRratio",df.col("Followers")/ df.col("Friends"))
#split_hashtags=df.select("Name").alias("name")
#hash_counts=split_hashtags.groupBy("name").count()
df.registerTempTable("TABLE")
new=spark.sql("SELECT Name as name,FRratio FROM TABLE order by FRratio desc limit 1")
query = new.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(50)
#query1.awaitTermination(50)

#print(csvDF.select("Id"))

#print(csvDF.isStreaming)
