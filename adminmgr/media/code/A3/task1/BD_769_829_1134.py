from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
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

tweets=inpTweets.select(explode(split(inpTweets.Hashtags,",")).alias("Hashtags"))
hashTagCount=tweets.groupBy("Hashtags").count()\
	.where("Hashtags!=''")\
	.orderBy("count",ascending=False)
result=hashTagCount.select("Hashtags","count").limit(5)
query = result \
		.writeStream \
		.outputMode("complete")\
		.format("console")\
		.start()
query.awaitTermination(60)
query.stop()


    