import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import lit


ctr=0
def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		
		crt=ctr+1
		#print("----------=========- %s -=========----------" % str(ctr),end='\n')
		try:
			sql_context = get_sql_context_instance(rdd.context)
			#row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			row_rdd = rdd.map(lambda w: Row(tweetid=w))
			#print(row_rdd.collect())
						


			hashtags_df = sql_context.createDataFrame(row_rdd)
			#hashtags_df.registerTempTable("hashtags")
			#hashtags_df.show()
			w1=hashtags_df.select(explode(split(hashtags_df.tweetid,",")).alias("hashtags"))
			#w1.show()
			w1.createOrReplaceTempView("updates1")
			w2 = sql_context.sql("select hashtags,count(*) from updates1 group by hashtags order by count(*) desc")
			#w2.show(3)
			r1=w2.rdd
			r1=r1.sortBy(lambda x:x[0],True)
			r1=r1.sortBy(lambda x:x[1],False)
			count=1
			for i,j in r1.collect():
				if(i==''):
					continue

				elif(count<5):
					print(i,end=',')
				elif(count==5):
					print(i,end='\n')
				else:
					break
				count=count+1
					
		except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)

def tmp(x):

	temp=x.split(';')
	#t1=temp[7].split(',')
	#print(t1)
	return temp[7]
	
	

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,int(sys.argv[2]))
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)




# dataStream.pprint()
tweet=dataStream.map(tmp)

#tweet1=tweet.window(int(sys.argv[1]),int(sys.argv[1]))
#dataStream.pprint()
# OR
#tweet=dataStream.map(lambda w:(w.split(';')[7].split(',')[0],1))
#count=tweet.reduceByKeyAndWindow(lambda x,y:x+y,0,int(sys.argv[1]))
#count=tweet.reduceByKey(lambda x,y:x+y)
#count=tweet.reduceByKey(lamnda x:x)
#count.pprint()

#TO maintain state
# totalcount=tweet.updateStateByKey(aggregate_tweets_count)
# totalcount.pprint()
ttw=tweet.window(int(sys.argv[1]))
#To Perform operation on each RDD
ttw.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(60)
ssc.stop()
