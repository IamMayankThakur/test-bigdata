import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.appName("Task1a").getOrCreate()

userSchema = StructType().add("id", "integer").add("lang", "string").add("date", "string").add("source", "string").add("len", "integer").add("likes", "integer").add("RTs", "string").add("hashtag", "string").add("umn", "string").add("umid", "string").add("name", "string").add("place", "string").add("followers", "integer").add("friends", "integer")

csvDF = spark.readStream.option("sep", ";").schema(userSchema).csv("/stream/")

csvDF = csvDF.select(explode(split(csvDF["hashtag"], ",")).alias("Hashtags")).where("hashtag != ''")

q = csvDF.groupBy("Hashtags").count().sort("count", ascending=False).limit(5)

query = q.writeStream.outputMode("complete").format("console").start()

query.awaitTermination(100)
query.stop()



