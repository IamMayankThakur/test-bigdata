# task1a
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .getOrCreate()

#"ID","language","Date","source","len","likes","RTs","Hashtags","Usernames","Userid","name","Place","followers","friends"
    user_schema = StructType().add("ID", "string").add("language", "string").add("Date", "string").add("source", "string").add("len", "string").add("likes", "integer").add("RTs", "integer").add("Hashtags", "string").add("Usernames", "string").add("Userid", "string").add("name", "string").add("Place", "string").add("followers", "integer").add("friends", "string")
    lines = spark\
        .readStream\
        .option("sep", ";")\
        .schema(user_schema)\
        .csv("hdfs://localhost:9000/stream")

    # Split the lines into words
    words = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.Hashtags, ',')
        ).alias('Hashtags')
    )

    # Generate running word count
    wordCounts = words.groupBy('Hashtags').count().orderBy("count", ascending = False).limit(5)
    # Start running the query that prints the running counts to the console
    query = wordCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    query.awaitTermination(60)
    query.stop()
