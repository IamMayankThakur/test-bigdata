from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType

spark = SparkSession.builder.appName("Pop_hashtag").getOrCreate()


userSchema = StructType().add("A", "string") \
		.add("B", "string") \
		.add("C", "string") \
		.add("D", "string") \
		.add("E", "string") \
		.add("F", "string") \
		.add("G", "string") \
		.add("H", "string") \
		.add("I", "string") \
		.add("J", "string") \
		.add("K", "string") \
		.add("L", "string") \
		.add("M", "string") \
		.add("N", "string")

csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream/")


hashtags = csvDF.select("H")
words = hashtags.select(explode(split(hashtags.H, ","))).withColumnRenamed("col", "Hashtags")


word = words.groupBy("Hashtags").count().orderBy("count", ascending = False).limit(5).writeStream.outputMode("complete").format("console").start()
word.awaitTermination(100)
word.stop()
spark.stop()
