import findspark 
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode
import pyspark.sql.functions as F

userSchema = StructType() \
			.add("id", "integer") \
			.add("Lang","string")	\
			.add("Date","string") \
			.add("Source","string") \
			.add("len","integer") \
			.add("likes","string")\
			.add("RT","string") \
			.add("Hashtags","string")\
			.add("UserMentionNames","string") \
			.add("UserMentionID","string") \
			.add("Friends","string") \

spark = SparkSession \
    .builder \
    .appName("Task1") \
    .getOrCreate()

lines = spark \
		.readStream \
		.schema(userSchema) \
		.csv("hdfs://localhost:9000/stream/",sep=";")


hashtags = lines.select(
   explode(
       F.split(lines.Hashtags,",")
   ).alias("Hashtags")
)

# kk=hashtags.sql("SELECT Hashtags,count(*) from ")
# hashtags_count =hashtags.groupby("Hashtags").agg(F.count("Hashtags").alias("count")).agg(F.max("count").alias("maximum"))
c_hashtags=hashtags \
	.groupby("Hashtags") \
	.count() \

output=c_hashtags.orderBy(F.desc("count")).limit(5)
# kk=wordcounts.agg({"count":"max"})


query = output \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination(100)
query.stop()
#spark.stop()