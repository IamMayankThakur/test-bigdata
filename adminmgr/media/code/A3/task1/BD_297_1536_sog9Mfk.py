from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql.functions import max
from pyspark.sql import SQLContext
from pyspark import SparkConf,SparkContext

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

data1=data.select(explode(split(data["hashtag"],",")).alias("Hashtags"))
sqlContext.registerDataFrameAsTable(data1, "data1")
data2= sqlContext.sql("SELECT Hashtags from data1 where Hashtags!=''")
c=data2.groupBy("Hashtags").count()

d=c.select("Hashtags","count").orderBy("count",ascending=False).limit(5)

query = d \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()


query.awaitTermination(60)
query.stop()
