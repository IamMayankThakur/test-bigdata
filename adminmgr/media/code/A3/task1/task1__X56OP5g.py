from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
# Create DataFrame representing the stream of input lines from connection to localhost:9999
# "ID","language","Date","source","len","likes","RTs","Hashtags","Usernames","Userid","name","Place","followers","friends"
userSchema = StructType().add("ID", "string").add("language", "string").add("source","string").add("len","string").add("likes","integer").add("RTs","integer").add("Hashtags","string").add("Usernames","string").add("Userid","string").add("name","string").add("Place","string").add("followers","string").add("friends","string")
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9009) \
    .schema(userSchema) \
    .csv("/stream/FIFA_modded_small_1.csv") \
    .load()

# Split the lines into words
# explode() takes in an array (or a map) as an input and outputs the elements of the array (map) as separate rows.
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Generate running word count
wordCounts = words.groupBy("word").count()
# Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
