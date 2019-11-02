# import findspark
# findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
	try:
		if(rdd.collect()):
			final = rdd.sortBy(lambda x:x[0])
			final1 =final.sortBy(lambda x:x[1] , ascending =False)
			#print(final1.collect())
			final_count = final1.map(lambda x:x[0])
			print(",".join(final_count.collect()[:5]))
	except:
			e = sys.exc_info()[0]
			#print("Error: %s" % e)
conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

a=int(sys.argv[1])
b=int(sys.argv[2])

ssc=StreamingContext(sc,1)
#ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
data = dataStream.window(a,b)
tweet=data.map(lambda w:(w.split(';')[7]))
tweet1=tweet.flatMap(lambda w:w.split(','))
tweet2=tweet1.map(lambda w:(w,1))

count=tweet2.reduceByKey(lambda x,y:x+y)
count1=count.filter(lambda x:x[0] is not '')
count1.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination(25)
ssc.stop()
