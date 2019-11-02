from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType

spark = SparkSession \
	.builder \
	.appName("Structured_Streaming") \
	.getOrCreate()

#Read all the csv files written atomically in a directory
userSchema = StructType().add("ID", "integer").add("lang", "string").add("Date", "string").add("Source", "string").add("len", "integer").add("Likes", "integer").add("RTs", "integer").add("hashtags", "string").add("UserMentionNames", "string").add("UserMentionID", "string").add("Name", "string").add("Place", "string").add("Followers", "integer").add("Friends", "integer")

csvDF = spark \
	.readStream \
	.schema(userSchema) \
	.csv("hdfs://localhost:9000/stream/",sep=";")

#csvDF.createOrReplaceTempView("tweets")

words = csvDF.select(
   explode(
       split(csvDF.Name, ",")
   ).alias("name"), (csvDF.Followers).alias("followers"), (csvDF.Friends).alias("friends")
)

words.createOrReplaceTempView("table")
res = spark.sql("select name, (sum(followers) / sum(friends)) as FRRatio from table group by name order by FRRatio DESC").limit(1)
#print(res)

query = res \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination(100)
query.stop()
