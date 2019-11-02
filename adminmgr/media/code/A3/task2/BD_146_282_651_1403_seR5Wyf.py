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


userSchema = StructType().add("ID", "string").add("Lang", "string").add("Date","date").add("Source","string").add("len","integer").add("likes","integer").add("RTs","integer").add("Hashtags","string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","integer").add("Friends","integer") 
df = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream")

df.createOrReplaceTempView("updates")
df2 = spark.sql("select Name as name, Followers/Friends as FRRatio from updates")
df2 = df2.groupby("name","FRRatio").count()
df2.createOrReplaceTempView("updates2")
popUser = spark.sql("select name, FRRatio from updates2 order by FRRatio desc limit 1")
#popUser = spark.sql("select name, FRRatio from updates2 where FRRatio = (select max('FRRatio') from updates)")

query = popUser \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination(60)
query.stop()