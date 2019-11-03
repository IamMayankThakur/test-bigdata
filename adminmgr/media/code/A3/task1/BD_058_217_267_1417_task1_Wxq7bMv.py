from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from pyspark.sql import SQLContext
from pyspark import SparkContext

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
sc = SparkContext.getOrCreate()

sqlContext = SQLContext(sc)
userSchema = StructType().add("ID", "integer").add("Lang", "string").add("Date", "string").add("Source", "string").add("Len", "integer").add("Likes", "integer").add("Rts", "integer").add("Hashtags", "string").add("Usernames", "string").add("UserID", "string").add("Name", "string").add("place", "string").add("followers", "integer").add("friends", "integer")

csvDF = spark \
    .readStream \
    .option("sep",';') \
    .schema(userSchema) \
    .csv("/stream") 

words = csvDF.select(
   explode(
       split(csvDF.Hashtags, ",")
   ).alias("Hashtags")
)

wordCounts = words.groupBy("Hashtags").count()
wc2 = wordCounts.orderBy("count",ascending = False).limit(5)

query = wc2 \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination(100)
query.stop()
