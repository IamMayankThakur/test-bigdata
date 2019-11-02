# import findspark
# findspark.init()
from __future__ import print_function
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import split, window
from pyspark.sql.types import StructType
from pyspark.sql.functions import desc


def myprint(x,y):
    x.show(5)
    '''b = x.first()
    print(b)
    print("lol")
    b.show()'''
    #a = x.rdd.collect()
    #print("%s,%s"%(a[0][0],a[0][1]))


# if len(sys.argv)!=4:
#     sys.exit(-1)


coulumns = 'ID, Lang, Date, Source, len, Likes, RTs, Hashtags, UserMentionnames, UserMentionID, name, Place, Followers, Friends'

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
    .add(col[12], "double") \
    .add(col[13], "double")



ssc = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()




# conf=SparkConf()
# conf.setAppname("BigData")
# sc=SparkContext(conf=conf)

# ssc=SQLContext(sc)
# ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.readStream.option("sep", ";").schema(schema).format("csv").load("/stream")
# tweet=dataStream.withWatermark("Date", "1 seconds").groupBy(window("Date", "1 seconds", "1 seconds"),"Hashtags").agg({'Hashtags' : 'count'})
tweet = dataStream.select("name","Followers","Friends")
tweet=tweet.select("name",(tweet.Followers/tweet.Friends).alias("FRRatio"))

hashcount = tweet.groupBy("name","FRRatio").count().orderBy(desc('FRRatio'))
hashcount = hashcount.select("name","FRRatio").limit(1)

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