from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
import time

userSchema = StructType().add("ID", "string").add("Lang", "string").add("Date", "string").add("Source", "string").add("len", "string").add("Likes", "string").add("RTs", "string").add("Hashtags", "string").add("UserMentionNames", "string").add("UserMentionID", "string").add("Name", "string").add("Place", "string").add("Followers", "string").add("Friends", "string")

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()


csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream")


csvDF.createOrReplaceTempView("updates")
agg2DF = spark.sql("select Name as name,sum(Followers)/sum(Friends) as FRRatio from updates group by name order by FRRatio DESC LIMIT 1")
query = agg2DF \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination(60)
query.stop()
