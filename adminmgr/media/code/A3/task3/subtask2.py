from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import csv

def pop(x):
	if x[2] != "0":
		return x[0],int(x[1])/int(x[2])
	else:
		return x[0],int(x[1])/1
	
	
	
sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()
#sc = SparkContext("local","app")
'''schema =StructType([
 	StructField("Id", StringType()),
        StructField("lang", StringType()),
        StructField("date", StringType()),
        StructField("source", StringType()),
	StructField("len", StringType()),
	StructField("likes", StringType()),
	StructField("rt", StringType()),
	StructField("ht", StringType()),
	StructField("umn", StringType()),
	StructField("umi", StringType()),
	StructField("name", StringType()),
	StructField("place", StringType()),
	StructField("followers", StringType())])'''
#home/chaitra/Downloads/FIFA_modded_small_1.csv
#hadoop://$HADOOP_HOME/spark/FIFA_modded_small_1.csv
'''fileStreamDf = sparkSession.readStream.option("sep",";").csv(tempfile.mkdtemp(),schema=schema)
print(fileStreamDF.isStreaming)'''
#df_load = sparkSession.read.csv('hdfs://bigdata.csv')

f = sparkSession.read.option("sep",";").csv('/home/chaitra/Downloads/FIFA_modded_small_1.csv').rdd.map(lambda y:[y[10],y[12],y[13]])
l=[]
for lines in f.collect():
	l.append(pop(lines))

'''for j in l:
	print(j)'''

ht1 = sparkSession.createDataFrame(l).rdd.distinct()#.cache()map(lambda x: tuple(x))
#hashTag = ht1.mapValues(lambda x: x)
for i in ht1.collect():
	print(i)
#print(type(ht1))
