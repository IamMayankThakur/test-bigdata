import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("BD_146_282_651_1403") \
    .getOrCreate()


uSchema = StructType().add("ID", "string").add("Lang", "string").add("Date","date").add("Source","string").add("len","integer").add("likes","integer").add("RTs","integer").add("Hashtags","string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","integer").add("Friends","integer") 
dataFrame = spark \
    .readStream \
    .option("sep", ";") \
    .schema(uSchema) \
    .csv("hdfs://localhost:9000/stream")

dataFrame.createOrReplaceTempView("updates")
dataFrame2 = spark.sql("select Name as name, Followers/Friends as FRRatio from updates")
dataFrame2 = dataFrame2.groupby("name","FRRatio").count()
dataFrame2.createOrReplaceTempView("updatesnew")
popUser = spark.sql("select name, FRRatio from updatesnew order by FRRatio desc limit 1")

query = popUser \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination(60)
query.stop()