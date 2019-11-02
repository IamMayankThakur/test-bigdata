#!/usr/bin/python3
import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from  operator import add
import sys
import requests
#import sqlContext.implicits._



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
			row_rdd = rdd.map(lambda w: Row(Hashtags=w[0], count=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select Hashtags, count from hashtags order by count desc")
			#hashtag_counts_df = sql_context.sql("select Hashtags, count from hashtags order by count desc")
			hashtag_counts_df.filter(hashtag_counts_df['Hashtags'] != '').show(n=5)
			#hashtag_counts_df.show()
		except:

			e = sys.exc_info()[0]
			print("Error: %s" % e)

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,2)
ssc.checkpoint("~/checkpoint_BIGDATA")


dataStream=ssc.socketTextStream("localhost",9009)

tweet=dataStream.map(lambda w:(w.split(';')[7])) #it gives the hashtags including the other columns which are comma separated
#print("Tweet-----------")
#tweet.pprint()
#print("\n")

tweet_2=tweet.map(lambda x:(x.split(',')[0],1))
#tweet_2=tweet.map(lambda w:((w.split(',')[0]),int(1)))
#print("Tweet-----------")
#tweet_2.pprint()
#print("\n")

#tweet_3 = tweet_2.filter(lambda a: (a[0] !="''"))
#print("Tweet-----------")
#tweet_3.pprint()


totalcount=tweet_2.updateStateByKey(aggregate_tweets_count)
#print("Totalcount")
#totalcount.pprint()

#To Perform operation on each RDD
totalcount.foreachRDD(process_rdd)
#print("totalcount_final-----------")
#totalcount.pprint()




#filter out the empty values: DONE
#sort it: Not yet done
# write the table: DONE
#print the first 5 values: DONE

#tweet_3=tweet_2.groupByKey()
#tweet_3=tweet_2.groupByKey()
#print("Tweet-----------")
#print(tweet_3.collect())
#tweet_3.pprint()
#print("\n")

#count=tweet_3.reduceByKey(add)
#print("Count-----------")
#count.pprint()

#tweet_f=tweet_s.flatMap(lambda r: tweet_ff(r)).groupByKey()
#tweet_4 = sorted(tweet_3, key = lambda var: var[0])
#tweet_4 = tweet_3.groupByKey()


#print("Tweet_final")
#tweet_4.pprint()
#tweet = tweet.flatMap(lambda k,v: (k.split(',')).datastream.map(lambda w:(w.split(';')[0]),1))
#print("Tweet_final-----------")
#tweet_final.pprint()

#count=tweet_4.reduceByKey(add)
#print("Count-----------")
#count.pprint()

#hashtag, 1

#TO maintain state
#totalcount=tweet_3.updateStateByKey(aggregate_tweets_count)
#print("totalcount-----------")
#totalcount.pprint()


#To Perform operation on each RDD
#totalcount.foreachRDD(process_rdd)
#print("totalcount_final-----------")
#totalcount.pprint()

'''
Solution:
dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.filter(lambda w:"Android" in w.split(';')[3])
#print("Tweet1--------")
#output: 
#1,01E+18;en;02-07-18 1:35;Twitter for Android;140;0;477;WorldCup,POR,ENG;Squawka Football;Squawka;Cayleb;Accra;861;828
tweet.pprint()
tweet=tweet.map(lambda w:("Tweets in android",1))

totalcount=tweet.updateStateByKey(aggregate_tweets_count)
totalcount.pprint()
'''
ssc.start()
ssc.awaitTermination(60)
query.stop()
ssc.stop()


