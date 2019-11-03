import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

	data = spark.sql("SELECT Hashtag,count(*) as count FROM tables WHERE Hashtag IS NOT NULL GROUP BY Hashtag ORDER BY count DESC limit 5")
	
	#data = dataFrame.select(posexplode(split("Hashtag",",")).alias("pos","Hastags"))

	#fData = data.groupBy("Hashtag").count().sort(col("count").desc()).limit(5)	

	query = data.writeStream\
		.outputMode("complete")\
		.format("console")\
		.start()		
								


	query.awaitTermination(100)

	query.stop()
