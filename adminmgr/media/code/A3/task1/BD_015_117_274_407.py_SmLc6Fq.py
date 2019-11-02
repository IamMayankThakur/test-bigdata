from pyspark.sql import Row, SQLContext
from pyspark import SparkConf, SparkContext
from operator import add
import requests
import sys
from pyspark.streaming import StreamingContext
#import findspark
from pyspark.sql import SparkSession,GroupedData
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode,split,count,desc
from pyspark.sql.functions import col


#findspark.init()

ssc = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

schema = StructType().add("ID", "string").add("Lang", "string") \
    .add("Date", "string").add("Source", "string").add("Len", "integer") \
    .add("Likes", "integer").add("RTs", "integer").add("Hashtags", "string") \
    .add("UserMentionName", "string").add("UserMentionID", "string").add("name", "string") \
    .add("Place", "string").add("Followers", "integer").add("Friends", "integer")

lines = ssc \
    .readStream \
    .schema(schema) \
    .option("sep", ";") \
    .csv("/stream")

words = lines.select(
    explode(
        split(lines.Hashtags, ",")
    ).alias("Hashtags"), "ID"
)

# lines.createOrReplaceTempView("frr")

hash1 = words.groupBy(words.Hashtags).count()
#.sort(col("count").desc())
#hash1.createOrReplaceTempView("hashtable")
newHash1 = hash1.select("*").orderBy(desc("count")).limit(5)
#hash1 = ssc.sql("select * from hashtable limit 5")

# frr = ssc.sql("select * from frr ")
# frr = lines.withColumn('FRRatio', frr.Followers/frr.Friends)
# frr=frr.groupBy((['name', frr.Followers, frr.Friends,frr.FRRatio])).count().sort(col("FRRatio").desc())
# frr = frr.drop("count")
# frr = frr.drop("Followers")
# frr = frr.drop("Friends")

# frr.createOrReplaceTempView("frr1")
# frr = ssc.sql("select * from frr1 limit 5")


query = newHash1.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination(100)
query.stop()
