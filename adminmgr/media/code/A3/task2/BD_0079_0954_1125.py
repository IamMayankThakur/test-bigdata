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
			.add("Name","string") \
			.add("Place","string") \
			.add("Followers","integer") \
			.add("Friends","integer") 

spark = SparkSession \
    .builder \
    .appName("Task1-2") \
    .getOrCreate()

lines = spark \
		.readStream \
		.schema(userSchema) \
		.csv("hdfs://localhost:9000/stream/",sep=";")


nameratio=lines.select("Name","Followers","Friends")

sumofall = nameratio.groupby("Name") \
					.sum("Followers","Friends")

columnrename=sumofall.withColumnRenamed("sum(Followers)","totalFollowers") \
					 .withColumnRenamed("sum(Friends)","totalFriends")

ratio = columnrename.select("name",(columnrename.totalFollowers/columnrename.totalFriends).alias("FRRatio")).orderBy(F.desc("FRRatio")).limit(1)

query = nameratio \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination(100)
query.stop()
# spark.stop()