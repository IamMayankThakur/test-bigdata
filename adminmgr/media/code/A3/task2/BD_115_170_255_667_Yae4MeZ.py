from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import *
import time


def process_row(row):
	max_count = 0
	max_hashtag = ""
	if(row['count']>max_count):
		max_count = row["count"]
		max_hashtag = row["word"]
	return max_count,max_hashtag

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

userSchema = StructType().add("ID","integer").add("Lang","integer").add("Date","string").add("Source","string").add("len","integer").add("Likes","integer").add("RTs","integer").add("Hashtags","string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","integer").add("Friends","integer")

csvDF = spark.readStream.option("sep", ";").schema(userSchema).csv("/stream")

csvDF.createOrReplaceTempView("Dataset")
tweets = spark.sql("select Hashtags from Dataset")

words = tweets.select(explode(split(tweets.Hashtags, ",")).alias("word"))

#df = words.readStream()

words.createOrReplaceTempView("Temp")
value = spark.sql("select word,count(word) as count from Temp group by word")

count = value.writeStream.format("console").outputMode('complete').foreach(process_row).start()

#print(max_hashtag,max_count)
#WC = words.groupBy("word").count()

count.awaitTermination(100)
count.stop()

