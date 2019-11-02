import findspark
findspark.init()
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import greatest
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import LongType
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import explode,split
import sys
import requests

inputPath="hdfs:///stream"
sc =SparkContext.getOrCreate()
spark=SparkSession(sc)

schema=StructType([StructField("ID",StringType(),True),
	StructField("Lang",StringType(),True),
	StructField("Date",StringType(),True),
	StructField("Source",StringType(),True),
	StructField("len",LongType(),True),
	StructField("Likes",LongType(),True),
	StructField("RTs",LongType(),True),
	StructField("Hashtags",StringType(),True),
	StructField("UserMentionNames",StringType(),True),
	StructField("UserMentionID",StringType(),True),
	StructField("Name",StringType(),True),
	StructField("Place",StringType(),True),
	StructField("Followers",LongType(),True),
	StructField("Friends",LongType(),True)])

inputDF=spark.readStream.schema(schema).option("sep",";").csv(inputPath)

inputDF.createOrReplaceTempView("data")



sqlDF3=spark.sql("SELECT Name,(Followers/Friends) as ratio FROM data")
sqlDF3.createOrReplaceTempView("ratio_h")
sqlDF4=spark.sql("SELECT Name as name,MAX(ratio) as FRRatio FROM ratio_h GROUP BY name ORDER BY FRRatio DESC LIMIT 1")


query2=sqlDF4.writeStream.format("console").outputMode("complete").start()

query2.awaitTermination(100) 
query2.stop()
spark.stop()


