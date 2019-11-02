import findspark
#findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import *
import requests
from pyspark.sql.functions import *
import sys


def prin(stream):
        """
        Print the first num elements of each RDD generated in this DStream.

        @param num: the number of elements from the first will be printed.
        """

        def takeAndPrint(time, rdd):
            
            i=0
            l=0
            taken = rdd.take(6)
            

            for record in taken:
            	if(record[0] == ''):
            		continue
            	else:
            		if(i<5):
	            		print(record[0],end="")
	            		l+=1
	            		i+=1

            	if(l<5):
                	print(",", end="")

			
        stream.foreachRDD(takeAndPrint)



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
			row_rdd = rdd.map(lambda w: Row(tag=w[0]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tag, count(*) as total from hashtags group by tag")
			hashtag_counts_df_new = sql_context.sql("select tag, total from hashtags_count_df order by total DESC")
			hashtag_counts_df_new.show()
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)

def tmp(x):
	return (x.split(';')[0],1)


def compute(rdd):
    for x in rdd:
        yield (x, 1)


bat_int= float(sys.argv[2])
wind= float(sys.argv[1])

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,bat_int)
#ssc.checkpoint("/Users/shreyasbs/Desktop/Academic/Sem5/BD/BDassn3/checkpoint/")

dataStream=ssc.socketTextStream("localhost",9009)
# dataStream.pprint()
#tweet=dataStream.map(tmp)
# OR
hashtag=dataStream.map(lambda w: w.split(';')[7].split(',')) 

hashtag= hashtag.transform(lambda x: x.flatMap(lambda x:compute(x)))


new_tag= hashtag.window(wind,1)

count=new_tag.reduceByKey(lambda x,y:x+y)
count_sorted= count.transform(lambda rdd: rdd.sortBy(lambda x: -x[1]))

prin(count_sorted)


#TO maintain state
# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()

#To Perform operation on each RDD
#tweet.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()