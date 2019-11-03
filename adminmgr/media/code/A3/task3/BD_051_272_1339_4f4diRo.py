import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import *

import re
import sys


if __name__ == "__main__":

	spark = SparkSession\
		.builder\
		.appName("Streaming")\
		.getOrCreate()

	#sockDf = spark\
	#	.readStream\
	#	.format("socket")\
	#	.option("host","localhost")

	mySchema = StructType()\
		.add("ID","string").add("Lang","string").add("Date","string").add("Source","string").add("len","integer").add("Likes","integer").add("RTs","integer").add("Hashtag","string").add("NameMention","string").add("MentionID","string").add("Name","string").add("Place","string").add("Followers","integer").add("Friends","integer")	

	dataFrame = spark.readStream\
		.option("sep",";")\
		.schema(mySchema)\
		.csv("/stream")
		#.load()


	dataFrame.createOrReplaceTempView("tables")

	table = spark.sql('SELECT *, count() FROM tables GROUP BY ' 			
			'ID,Lang,Date,Source,len,Likes,RTs,Hashtag,NameMention,MentionID,Name,Place,Followers,Friends')



	table.createOrReplaceTempView("secondTable")

	data = spark.sql("SELECT Name as name,Followers/Friends as FRRatio FROM secondTable ORDER BY FRRatio DESC LIMIT 1")
	
	#data = dataFrame.select(posexplode(split("Followers/Friends as FRRation",",")).alias("pos","FRRatio"))

	#fData = data.groupBy("Hashtag").count().sort(col("count").desc()).limit(5)	

	query = data.writeStream\
		.outputMode("complete")\
		.format("console")\
		.start()		
								


	query.awaitTermination(60)

	query.stop()
