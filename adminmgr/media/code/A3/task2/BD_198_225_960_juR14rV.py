from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

df=StructType([StructField("ID", IntegerType(), True),
    StructField("Lang", StringType(), True),
    StructField("Date", DateType(), True),
    StructField("Source", StringType(), True),
    StructField("len", IntegerType(), True),
    StructField("Likes", IntegerType(), True),
    StructField("RTs", IntegerType(), True),
    StructField("Hashtags", StringType(), True),
    StructField("UserMentionNames", StringType(), True),
    StructField("UserMentionID", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Place", StringType(), True),
    StructField("Followers", IntegerType(), True),
    StructField("Friends", IntegerType(), True)]
)

streamingInputDF = (
  spark
    .readStream                                       
    .csv("hdfs:///stream", schema=df, sep=";")
)

streamingInputDF.createOrReplaceTempView("updates")
a=spark.sql("select Name as name, Round(max(Followers)/max(Friends),1) AS FRRatio from updates group by Name order by FRRatio DESC LIMIT 1")


var=a.writeStream.outputMode("complete").format("console").start()
var.awaitTermination(100)
var.stop()
spark.stop()
