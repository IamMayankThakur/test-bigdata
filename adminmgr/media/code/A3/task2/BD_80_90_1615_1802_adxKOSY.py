from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *

#Create A spark Session
spark = SparkSession.builder.appName("modeHashtags").getOrCreate()


#Creating A userSchema
spark_schema = StructType().add("Id", "string").add("Lang", "string").add("Date", "string").add("Source", "string").add("len", "integer").add("Likes", "integer").add("RTs", "string").add("hashs", "string").add("UserMentionNames", "string").add("UserMentionID", "integer").add("Name", "string").add("Place", "string").add('Followers', "integer").add('Friends', "integer")



df = spark.readStream.option("sep", ";").schema(spark_schema).csv("hdfs://localhost:9000/stream")
#df=df.withColumn("FRratio",df[Followers]/df[Friends])
#hash_counts=df.groupBy(["Name","FRratio"]).count()
#df=df.select("Name","Followers","Friends")
df=df.withColumn("FRRatio",df.Followers/df.Friends)
#df.registerTempTable("TABLE")
df=df.select("Name","FRRatio")
df=df.groupBy(["Name","FRRatio"]).count()
df.registerTempTable("TABLE")
new=spark.sql("SELECT Name as name,FRRatio FROM TABLE order by FRRatio desc limit 1")
query = new.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(60)
query.stop()
#query1.awaitTermination(50)

#print(csvDF.select("Id"))

#print(csvDF.isStreaming)
