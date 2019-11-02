from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import posexplode
from pyspark.sql.functions import count
from pyspark.sql.functions import col
from pyspark.sql.functions import desc

spark = SparkSession \
        .builder \
        .appName("MostCommonHashtag") \
        .getOrCreate()

userSchema = StructType() \
    .add("id", "string") \
    .add("lang", "string") \
    .add("date", "string") \
    .add("source", "string") \
    .add("len", "integer") \
    .add("likes", "integer") \
    .add("rts", "string") \
    .add("hashtags", "string") \
    .add("unames", "string") \
    .add("uid", "string") \
    .add("name", "string") \
    .add("place", "string") \
    .add("followers", "integer") \
    .add("friends", "integer")

csvDF = spark \
        .readStream \
        .option("sep", ";") \
        .schema(userSchema) \
        .csv("hdfs://localhost:9000/stream/")

ratioDF = csvDF.withColumn('FRRatio', csvDF['followers'] / csvDF['friends'])

ratioDF = ratioDF.groupBy("name", "FRRatio").count()

res = ratioDF.orderBy(desc("FRRatio")).limit(1).select("name", "FRRatio")

query = res \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination(60)
query.stop()
