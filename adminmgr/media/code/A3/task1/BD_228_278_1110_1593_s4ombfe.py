from pyspark.sql import SparkSession
#from pyspark import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import *
if __name__ == "__main__":
	spark = SparkSession\
	.builder\
	.appName("PythonPageRank")\
	.getOrCreate()
	userSchema = StructType().add("ID", "string").add("Lang", "string").add("Date","string").add ("Source","string").add("len","integer").add("Likes","integer").add("RTs","integer").add("Hashtag","string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","integer").add("Friends","integer")
	csvDF = spark \
    	.readStream \
    	.option("sep", ";") \
    	.schema(userSchema) \
    	.csv("/stream")
	csvDF.createOrReplaceTempView("tweets")
	hashtags=csvDF.select("Hashtag")
	words = hashtags.select(
        # explode turns each item in an array into a separate row
        explode(
            split(hashtags.Hashtag, ',')
        ).alias('Hashtags')
    	)
	wordCounts = words.groupBy('Hashtags').count()
	wordCounts.createOrReplaceTempView("hashtags")
	#sqlDF = spark.sql("SELECT * FROM tweets")
	sortedwords = spark.sql("SELECT Hashtags,count FROM hashtags ORDER BY count DESC LIMIT 5")
	query=sortedwords.writeStream.outputMode('complete').format("console").start()
	#sleep(10)
	query.awaitTermination(100)
	#csvDF.show()
	query.stop()
