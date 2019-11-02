from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import IntegerType, StringType, StructType
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import desc, col
#import org.apache.spark.sql.functions._
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
    

'''words=csvDF.select(explode(split(csvDF.Hashtags,",")).alias("hashtag"))
	
wordcounts=words.groupBy("hashtag").count().select("hashtag","count")
wc=wordcounts.orderBy(desc("count")).select("hashtag")
#ws=wordcounts..show(10)
'''
ratio = csvDF.select("name",(col("Followers") / col("Friends")).alias("FRRatio"))
ratio=ratio.groupBy("name","FRRatio").count()
r=ratio.orderBy(desc("FRRatio")).select(col("name"),col("FRRatio")).limit(1)
#ratio = csvDF.select("ratio")
#print("sorted bruh")
query=r \
	.writeStream \
	.outputMode ("append") \
	.format("console") \
	.start()
query.awaitTermination(60)
query.stop()
'''
	
s=csvDF.groupBy("Hashtags").count() \
  .writeStream \
  .outputMode("complete") \
  .format("console") \
  .start()
'''  
  
'''
s \
  .writeStream \
  .queryName("aggregates") \
  .outputMode("complete") \
  .format("memory") \
  .start()
  
spark.sql("select * from aggregates").show()

#query=s.writeStream.format("console").start()

#import time
#time.sleep(10)
#query.stop()

'''
	
