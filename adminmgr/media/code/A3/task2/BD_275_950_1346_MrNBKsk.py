from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, desc, col
from pyspark.sql.types import StructType

if __name__ == "__main__":
    """if len(sys.argv) != 3:
        print("Usage: structured_network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])"""

    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to host:port
    """lines = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .load()"""

#"ID","language","Date","source","len","likes","RTs","Hashtags","Usernames","Userid","name","Place","followers","friends"
    user_schema = StructType().add("ID", "string").add("language", "string").add("Date", "string").add("source", "string").add("len", "string").add("likes", "integer").add("RTs", "integer").add("Hashtags", "string").add("Usernames", "string").add("Userid", "string").add("name", "string").add("Place", "string").add("followers", "integer").add("friends", "integer")
    lines = spark\
        .readStream\
        .option("sep", ";")\
        .schema(user_schema)\
        .csv("hdfs://localhost:9000/stream")

    # Split the lines into words
    words = lines.select(
        # explode turns each item in an array into a separate row
        lines.name, lines.friends/lines.followers
    )

    # Generate running word count
    wordCounts = words.groupBy(lines.name).agg({"(friends / followers)": "max" }).limit(5)
    wordCounts = wordCounts.select(col("name"), col("max((friends / followers))").alias("FRRatio"))
    # Start running the query that prints the running counts to the console
    query = wordCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    query.awaitTermination(60)
	query.stop()
 
