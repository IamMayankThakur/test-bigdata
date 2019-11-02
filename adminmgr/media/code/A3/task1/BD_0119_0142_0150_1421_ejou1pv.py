from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from pyspark.sql import window
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("Most popular hashtag").getOrCreate()


userSchema = StructType().add("1", "string") \
		.add("2", "string") \
		.add("3", "string") \
		.add("4", "string") \
		.add("5", "string") \
		.add("6", "string") \
		.add("7", "string") \
		.add("c8", "string") \
		.add("9", "string") \
		.add("10", "string") \
		.add("11", "string") \
		.add("12", "string") \
		.add("c13", "string") \
		.add("c14", "string")

csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/A3/new/")


hashtags = csvDF.select("c8")
words = hashtags.select(explode(split(hashtags.c8, ",")))
words = words.withColumnRenamed("col", "Hashtags")


word = words.groupBy("Hashtags").count().orderBy("count", ascending = False).limit(5).writeStream.outputMode("complete").format("console").start()
word.awaitTermination(30)
word.stop()
spark.stop()
