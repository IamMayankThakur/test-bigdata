from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import *

if __name__ == "__main__":
	spark = SparkSession\
	.builder\
	.appName("SparkStream")\
	.getOrCreate()
	userSchema = StructType().add("ID", "string").add("Lang", "string").add("Date","string").add ("Source","string").add("len","integer").add("Likes","integer").add("RTs","integer").add("Hashtags","string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","integer").add("Friends","integer")
	df = spark \
    	.readStream \
    	.option("sep", ";") \
    	.schema(userSchema) \
    	.csv("/stream")
	df.createOrReplaceTempView("table")
	df1 = spark.sql("SELECT Hashtags,count(*) as count FROM table WHERE Hashtags IS NOT NULL GROUP BY Hashtags ORDER BY count DESC LIMIT 5")
	query=df.writeStream.outputMode('complete').format("console").start()
	query.awaitTermination(100)
	query.stop()
