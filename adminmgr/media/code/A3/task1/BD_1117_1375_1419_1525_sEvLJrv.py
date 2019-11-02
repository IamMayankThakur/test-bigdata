# import findspark
# findspark.init()
from __future__ import print_function
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, window
from pyspark.sql.types import StructType
from pyspark.sql.functions import desc


def myprint(x,y):
    x.show(5)
    #a = x.rdd.collect()[0]
    #print("%s,%s"%(a[0],a[1]))


# if len(sys.argv)!=4:
#     sys.exit(-1)


coulumns = 'ID, Lang, Date, Source, len, Likes, RTs, Hashtags, UserMentionNames, UserMentionID, Name, Place, Followers, Friends'

col = coulumns.split(', ')

schema = StructType().add(col[0], "integer") \
    .add(col[1], "string") \
    .add(col[2], "timestamp") \
    .add(col[3], "string") \
    .add(col[4], "integer") \
    .add(col[5], "integer") \
    .add(col[6], "integer") \
    .add(col[7], "string") \
    .add(col[8], "string") \
    .add(col[9], "string") \
    .add(col[10], "string") \
    .add(col[11], "string") \
    .add(col[12], "integer") \
    .add(col[13], "integer")



ssc = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()




# conf=SparkConf()
# conf.setAppName("BigData")
# sc=SparkContext(conf=conf)

# ssc=SQLContext(sc)
# ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.readStream.option("sep", ";").schema(schema).format("csv").load("/stream")
# tweet=dataStream.withWatermark("Date", "1 seconds").groupBy(window("Date", "1 seconds", "1 seconds"),"Hashtags").agg({'Hashtags' : 'count'})
tweet = dataStream.select("Hashtags")
tweet=tweet.select(explode(split(tweet.Hashtags, ",")).alias("Hashtags"))

hashcount = tweet.groupBy("Hashtags").count().orderBy(desc('count')).limit(5)

query = hashcount.writeStream.outputMode("complete").format("console").start()

query.awaitTermination(100)


# tweet=dataStream.select("Date","Hashtags").groupBy(window("Date","1 days","1 days"),"Hashtags").agg({"Hashtags":"count"}).orderBy("Hashtags")
# print 'lol'
# tweet.writeStream.outputMode('complete').format('console').start().awaitTermination()







# tweet.pprint()
# tweet=tweet.count()

# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()


query.stop()
ssc.stop()