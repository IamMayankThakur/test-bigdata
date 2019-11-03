import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType,StringType,IntegerType,StructField

directory = "hdfs://localhost:9000/stream"
#directory = "/home/hduser/Desktop/Assignment3/Input"

spark = SparkSession.builder.appName("Most common hashtag").getOrCreate()

tweetSchema = StructType().add("id","string").add("lang","string").add("date","string").add("source","string").add("len","integer").add("likes","integer").add("rts","integer").add("Hashtags","string").add("usermentionnames","string").add("usermentionid","string").add("name","string").add("place","string").add("followers","integer").add("friends","integer")

tweetdf = spark.readStream.option("sep",";").schema(tweetSchema).csv(directory)

hashtags = tweetdf.select("Hashtags").where("Hashtags is not null")

query = hashtags.groupby("Hashtags").count().orderBy("count",ascending=False).limit(5).writeStream.format("console").outputMode("complete").start()

query.awaitTermination(60)
query.stop()
