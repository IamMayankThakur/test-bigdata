from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
import pyspark.sql.functions as func

spark = SparkSession\
        .builder\
        .appName("Task1")\
        .getOrCreate()

userSchema=StructType().add("ID","integer").add("language","string") \
				.add("Date","string").add("source","string").add("len","integer").add("likes","string") \
				.add("RTs","string").add("Hashtags","string").add("Usernames","string").add("Userid","string") \
				.add("name","string").add("Place","string").add("followers","integer").add("friends","integer")

csvfilepath="hdfs://localhost:9000/stream/"

inpTweets = spark \
	.readStream.option("sep", ";") \
	.schema(userSchema) \
	.csv(csvfilepath)

inpTweets=inpTweets.withColumn("friends",tweets["friends"].cast(DoubleType()))
inpTweets=inpTweets.withColumn("followers",tweets["followers"].cast(DoubleType()))
inpTweets=inpTweets.withColumn("ratio",col("followers")/col("friends"))
popTweets=inpTweets.select("name","ratio")
fratio=popTweets.groupBy("name")\
	.agg(func.max("ratio").alias("FRRatio"))
result=fratio.select("name","FRRatio").orderBy("FRRatio",ascending=False).limit(1)
query = result \
		.writeStream \
		.outputMode("complete")\
		.format("console")\
		.start()
query.awaitTermination(60)
query.stop()
