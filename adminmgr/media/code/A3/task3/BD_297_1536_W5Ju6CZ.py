# import findspark
# findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys

def process_rdd(time, rdd):
	try:
		#if(rdd.collect()):
			final = rdd.sortBy(lambda x:x[0])
			final1 =final.sortBy(lambda x:x[1] , ascending =False)
			#print(final1.collect())
			final_count = final1.map(lambda x:x[0]).collect()[:5]
			print(",".join(final_count))
	except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

a=int(sys.argv[1])
b=int(sys.argv[2])

ssc=StreamingContext(sc,1)
ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
tweet=dataStream.map(lambda w:w.split(';')[7])
tweet1=tweet.flatMap(lambda w:(w.split(',')))
tweet2=tweet1.map(lambda w:(w,1))

count=tweet2.reduceByKeyAndWindow(lambda x,y:x+y , lambda x,y:x-y,a,b)
count1=count.filter(lambda x:x[0] is not '')
count1.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
