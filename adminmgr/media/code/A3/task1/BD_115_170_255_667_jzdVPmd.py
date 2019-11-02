from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

userSchema = StructType().add("ID","integer").add("Lang","integer").add("Date","string").add("Source","string").add("len","integer").add("Likes","integer").add("RTs","integer").add("Hashtags","string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","integer").add("Friends","integer")

csvDF = spark.readStream.option("sep", ";").schema(userSchema).csv("/stream")

'''
csvDF.createOrReplaceTempView("Dataset")
tweets = spark.sql("select Hashtags, Source from Dataset")
'''

words = csvDF.select(explode(split(csvDF.Hashtags, ",")).alias("word"), csvDF.Date.alias("Date"))

#count = words.writeStream.format("console").outputMode("append").start()
#print(tweets)

words.createOrReplaceTempView("Hashtags_o")

value = spark.sql("select word as Hashtags,count(Date) as count as value from Hashtags_o group by word order by value DESC").limit(5)

count = value.writeStream.format("console").outputMode("complete").start()

count.awaitTermination(25)
count.stop()
