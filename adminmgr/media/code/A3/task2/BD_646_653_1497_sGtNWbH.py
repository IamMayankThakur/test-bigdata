import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode,desc
from pyspark.sql.functions import split
import pyspark.sql.functions as F
from pyspark.sql.functions import col


def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tweetid, no_of_tweets from hashtags")
			hashtag_counts_df.show()
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)


conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,2)
ssc.checkpoint("/home/prerana/checkpoint_BIGDATA")

spark = SparkSession\
        .builder\
        .appName("BigData")\
        .getOrCreate()
userschema=StructType().add("ID","string").add("Lang","string").add("Date","string").add("Source","string").add("len","integer").add("Likes","integer").add("RTs","integer").add("Hashtags","string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","integer").add("Friends","integer")

spark1=spark\
       .readStream\
       .schema(userschema)\
       .format("csv")\
       .option("sep",";")\
       .load("hdfs://localhost:9000/stream/")

data=spark1.select(spark1.Name,spark1.Followers/spark1.Friends)


data = data.select(col("Name").alias("name"), col("(Followers / Friends)").alias("FRRatio"))

finalcounts = data.groupBy("name").max().sort(desc("max(FRRatio)")).limit(1)
finalcounts=finalcounts.select(col("Name").alias("name"), col("max(FRRatio)").alias("FRRatio"))

#wordCounts = words.groupBy("hash").count().sort(desc("count")).limit(1)

query = finalcounts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(60)
query.stop()
