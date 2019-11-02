from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.types import StructType
from pyspark.sql.functions import desc

if __name__ == "__main__":


    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCountWindowed")\
        .getOrCreate()


    userSchema = StructType().add("Id", "integer").add("Lang", "string").add("Date", "date").add("Source", "string").add("len", "integer").add("Likes", "integer").add("RTs", "string").add("Hashtags", "string").add("UserMentionNames", "string").add("UserMentionId", "integer").add("Name", "string").add("Place", "string").add("Followers", "integer").add("Friends", "integer")
    

    lines = spark \
        .readStream \
        .option("sep", ";") \
        .schema(userSchema) \
        .csv("hdfs://localhost:9000/stream/")
   #print(lines)

    words = lines.select(
	lines.Name.alias("name"),
	lines.Followers.alias("followers"),
	lines.Friends.alias("friends"),
	(lines.Followers/lines.Friends).alias("ratio")
     )

# Generate running word count
    #wordCounts = words.groupBy("name").sum("ratio").withColumnRenamed('sum(ratio)','ratios')
    wordC = words.groupBy("name").sum("ratio").withColumnRenamed('sum(ratio)','FRRatio')
    wordCounts=wordC.orderBy(desc('FRRatio')).limit(1)
    
    q = wordCounts.writeStream.outputMode("complete").format("console").start()
    #q = wordCounts.writeStream.outputMode("complete").format("console").start()  #use complete output mode when using groupby or use append 
    q.awaitTermination(60)
    q.stop()

    # Split the lines into words, retaining timestamps
    # split() splits each line into an array, and explode() turns the array into multiple rows
    '''words = lines.select(
        explode(split(lines.value, ' ')).alias('word'),
        lines.timestamp
    )

    # Group the data by window and word and compute the count of each group
    windowedCounts = words.groupBy(
        window(words.timestamp, windowDuration, slideDuration),
        words.word
    ).count().orderBy('window')

    # Start running the query that prints the windowed word counts to the console
    query = windowedCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .option('truncate', 'false')\
        .start()

    query.awaitTermination()'''
