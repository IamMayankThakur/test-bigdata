from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("A3T1b").getOrCreate()
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
df2 = df.select("date","name","followers","friends")
df3 = df2.withColumn("FRRatio",df["followers"]/df["friends"]).select("name","FRRatio").groupBy("name","FRRatio").count().sort(col("FRRatio").desc()).limit(1)
df4 = df3.select("name","FRRatio")
q = df4.writeStream.outputMode("complete").format("console").start()
q.awaitTermination(100)
q.stop()