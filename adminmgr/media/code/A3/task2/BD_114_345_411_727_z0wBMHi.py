from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType

spark = SparkSession \
	.builder \
	.appName("Structured_Streaming") \
	.getOrCreate()

#Read all the csv files written atomically in a directory
userSchema = StructType().add("ID", "integer").add("lang", "string").add("Date", "string").add("Source", "string").add("len", "integer").add("Likes", "integer").add("RTs", "integer").add("hashtags", "string").add("UserMentionNames", "string").add("UserMentionID", "string").add("Name", "string").add("Place", "string").add("Followers", "string").add("Friends", "string")

csvDF = spark \
	.readStream \
	.schema(userSchema) \
	.csv("hdfs://localhost:9000/stream/",sep=";")

#func = csvDF.select("ID").where("ID == 5")
words = csvDF.select(
   explode(
       split(csvDF.hashtags, ",")
   ).alias("Hashtags")
)

# Generate running word count
wordCounts = words.groupBy("Hashtags").count()

wordCounts.createOrReplaceTempView("word_count")

fin = spark.sql("select Hashtags, count from word_count order by count DESC").limit(5)
query = fin \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination(100)
query.stop()
