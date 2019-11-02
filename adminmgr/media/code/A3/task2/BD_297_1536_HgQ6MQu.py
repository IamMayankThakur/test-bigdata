from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql import SQLContext
from pyspark import SparkConf,SparkContext
from pyspark.sql.functions import max
from pyspark.sql.functions import col

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)
sqlContext = SQLContext(sc)

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

userSchema = StructType().add("id", "integer").add("lang", "string").add("date","timestamp").add("source","string").add("len","integer").add("likes","integer").add("rt","integer").add("hashtag","string").add("username","string").add("userid","string").add("name","string").add("place","string").add("followers","integer").add("friends","integer")

data = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream")


sqlContext.registerDataFrameAsTable(data, "ratios")
f= sqlContext.sql("SELECT name, followers/friends AS FRRatio from ratios")
e=f.groupBy("name").agg({"FRRatio":"max"})
g=e.orderBy("max(FRRatio)",ascending=False).limit(1)
h= g.select(col("name").alias("name"),col("max(FRRatio)").alias("FRRatio"))

query = h \
    .writeStream \
    .outputMode("complete")\
    .format("console") \
    .start()


query.awaitTermination(60)
query.stop()
