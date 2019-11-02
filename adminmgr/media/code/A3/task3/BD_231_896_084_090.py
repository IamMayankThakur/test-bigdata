from __future__ import print_function

import sys
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    # if len(sys.argv) != 3:
    #     print("Usage: structured_network_wordcount.py <hostname> <port>", file=sys.stderr)
    #     sys.exit(-1)

    # host = sys.argv[1]
    # port = int(sys.argv[2])

    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .getOrCreate()

    schemaString = "ID, Lang, Date, Source, len, Likes, RTs, Hashtags, UserMentionNames, UserMentionID, Name, Place, Followers, Friends"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(", ")[:-2]] + [StructField(field_name, IntegerType(), True) for field_name in schemaString.split(", ")[-2:]]
    userSchema = StructType(fields)

    lines = spark\
    .readStream\
    .option("header","false")\
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("/stream")  # Equivalent to format("csv").load("/path/to/directory")

    hasht = lines.select(["Name","Followers","Friends"])

    top = hasht.withColumn("FRRatio", hasht.Followers/hasht.Friends).drop("Followers","Friends")

    max_value = top.agg({"FRRatio": "max"})
    top_ = top.join(max_value,top["FRRatio"] == max_value["max(FRRatio)"],'inner')

    # Start running the query that prints the running counts to the console
    query = top_\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    query.awaitTermination(100)
    query.stop()