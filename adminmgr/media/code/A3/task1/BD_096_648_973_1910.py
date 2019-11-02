from __future__ import print_function

import sys
import findspark
findspark.init()
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType

if __name__ == "__main__":
    #if len(sys.argv) != 3:
    #    print("Usage: structured_network_wordcount.py <hostname> <port>", file=sys.stderr)
    #    sys.exit(-1)

    #host = sys.argv[1]
    #port = int(sys.argv[2])

    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .getOrCreate()

    # Read all the csv files written atomically in a directory
    userSchema = StructType().add("id" , "string").add("lang" , "integer").add("Date" , "string").add("Source" , "string").add("len" , "integer").add("Likes" , "integer").add("RT" , "integer").add("Hashtags" , "string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","integer").add("Friends","integer")
    csvDF = spark \
        .readStream \
        .option("sep", ";") \
        .schema(userSchema) \
        .csv("hdfs://localhost:9000/stream")  # Equivalent to format("csv").load("/path/to/directory")

    # Split the lines into words
    csvDF.createOrReplaceTempView("updates")
    hasht=spark.sql("select Hashtags from updates")
    words = hasht.select(
        explode(
            split(hasht.Hashtags, ",")
        ).alias("Hashtags")
    )

    # Generate running word count
    wordCounts = words.groupBy("Hashtags").count()
    wordCounts.createOrReplaceTempView("mch")
    bla=spark.sql("select * from mch order by count desc limit 5")
    #mch=bla.first()
    #mostcommonhash=spark.sql("select max(Hashtags) from mch")
    

    # Start running the query that prints the running counts to the console
    query = bla\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    query.awaitTermination(60)
    query.stop()
