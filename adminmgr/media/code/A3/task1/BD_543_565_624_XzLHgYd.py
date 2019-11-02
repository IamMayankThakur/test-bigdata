from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import IntegerType, StringType, StructType
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import desc, col
spark = SparkSession \
    .builder \
    .appName("Task1") \
    .getOrCreate()

'''
userSchema = StructType().add("name", "string").add("age", "integer").add("ID","string"), Lang, Date, Source, len, Likes, RTs, Hashtags, UserMentionNames, UserMentionID, Name, Place, Followers,
'''
userSchema = StructType() \
	.add("ID","string") \
	.add("Lang","string") \
	.add("Date","string") \
	.add("Source","string") \
	.add("len","string") \
	.add("Likes","string") \
	.add("RTs","string") \
	.add("Hashtags","string") \
	.add("UserMentionNames","string") \
	.add("UserMentionID","string") \
	.add("Name","string") \
	.add("Place","string") \
 	.add("Followers","integer") \
	.add("Friends","integer") 

csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream/") 
    

words=csvDF.select(explode(split(csvDF.Hashtags,",")).alias("Hashtags"))
	
wordcounts=words.groupBy("Hashtags").count().select("Hashtags","count")
wc=wordcounts.orderBy(desc("count")).select("Hashtags","count").limit(5)

query=wc \
	.writeStream \
	.outputMode ("complete") \
	.format("console") \
	.start()
query.awaitTermination(60)
query.stop()

