import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
# Create DataFrame representing the stream of input lines from connection to localhost:9999
#("ID","language","Date","source","len","likes","RTs","Hashtags","Usernames","Userid","name","Place","followers","friends")
userSchema = StructType().add("ID", "string").add("language", "string").add("Date", "string").add("source", "string").add("len", "string").add("likes", "string").add("RTs", "string").add("Hashtags", "string").add("Usernames", "string").add("Userid", "string").add("name", "string").add("Place", "string").add("followers", "string").add("friends", "string")
csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv('/stream')
hCounts = csvDF.groupBy("Hashtags").count().orderBy("count", ascending=0)
hCounts.createOrReplaceTempView("updates")
hCounts=spark.sql("select * from updates LIMIT 5")
query = hCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
	.option("numRows",'5') \
    .start()

query.awaitTermination(100)
query.stop()
