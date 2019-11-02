import findspark 
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession, Window
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField
from pyspark.sql.functions import explode, split, max, rank, min, current_timestamp, expr

spark = SparkSession.builder.appName("Task1b").getOrCreate()

userSchema = StructType().add("id", "integer").add("lang", "string").add("date", "string").add("source", "string").add("len", "integer").add("likes", "integer").add("RTs", "string").add("hashtags", "string").add("umn", "string").add("umid", "string").add("name", "string").add("place", "string").add("followers", "integer").add("friends", "integer")

csvDF = spark.readStream.option("sep", ";").schema(userSchema).csv("/stream")

csvDF = csvDF.withColumn("ratio", csvDF.followers/csvDF.friends).where("followers != 0 and friends != 0")

q = csvDF.groupBy('name').agg(max('ratio').alias("FRRatio")).sort("FRRatio", ascending=False).limit(1)

query = q.writeStream.outputMode("complete").format("console").start()

query.awaitTermination(100)
query.stop()

