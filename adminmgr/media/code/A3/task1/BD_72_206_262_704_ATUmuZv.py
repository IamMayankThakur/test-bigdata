from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("A3T1a").getOrCreate()
socketDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9000) \
    .load()

schema = StructType()\
        .add("id",StringType())\
        .add("lang",StringType())\
        .add("date",DateType())\
        .add("source",StringType())\
        .add("len",IntegerType())\
        .add("likes",IntegerType())\
        .add("rts",IntegerType())\
        .add("hashtags",StringType())\
        .add("usermentionnames",StringType())\
        .add("usermentionid",StringType())\
        .add("name",StringType())\
        .add("place",StringType())\
        .add("followers",IntegerType())\
        .add("friends",IntegerType())

df = spark.readStream.option("sep",';').schema(schema).csv("/stream")
df2 = df.select(posexplode(split("hashtags",",")).alias("pos","Hashtags"))
df3 = df2.groupBy("Hashtags").count().sort(col("count").desc()).limit(5)
q = df3.writeStream.outputMode("complete").format("console").start()
q.awaitTermination(100)
q.stop()