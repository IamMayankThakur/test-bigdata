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
df2 = df.withColumn("ratio",df["followers"]/df["friends"]).select("name","ratio").groupBy("name","ratio").count().sort(col("ratio").desc()).limit(1)
df2.writeStream.outputMode("complete").format("console").start().awaitTermination(40)