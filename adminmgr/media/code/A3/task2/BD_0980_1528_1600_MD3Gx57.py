from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
spark = SparkSession.builder.appName("tweets").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
userSchema = StructType()\
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

dfCSV = spark.readStream.option("sep", ";").option("header", "false").schema(userSchema).csv("/stream")
dfCSV.createOrReplaceTempView("tweets")

q2 = spark.sql("select name,max(followers/friends) as FRRatio from tweets group by name order by FRRatio desc limit 1")
query = q2.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(100)
query.stop()