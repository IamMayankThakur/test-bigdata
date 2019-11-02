from pyspark.sql.types import StructType
import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

#spark = SparkSession

spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .getOrCreate()

#ID, Lang, Date, Source, len, Likes, RTs, Hashtags, UserMentionNames, UserMentionID, Name, Place, Followers, Friends
# Read all the csv files written atomically in a directory
userSchema = StructType().add("ID", "integer").add("Lang", "string").add("Date", "string").add("Source", "string").add("len", "integer").add("Likes", "integer").add("RTs", "string").add("Hashtags", "string").add("UserMentionNames", "string").add("UserMentionID", "integer").add("Name", "string").add("Place", "string").add("Followers", "integer").add("Friends", "integer")
csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("/stream")  # Equivalent to format("csv").load("/path/to/directory")

csvDF.createOrReplaceTempView("table")
#print(csvDF.isStreaming)
a=spark.sql("SELECT Name,Round(  (max(Followers)/max(Friends)) ,1) as FRRatio FROM table GROUP BY Name ORDER BY FRRatio desc LIMIT 5")
b = a.writeStream.outputMode("complete").format("console").start()

b.awaitTermination(60)
b.stop()
