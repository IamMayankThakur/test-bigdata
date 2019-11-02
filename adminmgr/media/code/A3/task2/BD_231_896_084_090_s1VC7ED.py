from __future__ import print_function

import sys
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("BD_Assignment3_Task1_2")\
        .getOrCreate()

    schemaString = "ID, Lang, Date, Source, len, Likes, RTs, Hashtags, UserMentionNames, UserMentionID, name, Place, Followers, Friends"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(", ")[:-2]] + [StructField(field_name, DoubleType(), True) for field_name in schemaString.split(", ")[-2:]]
    userSchema = StructType(fields)

    lines = spark\
    .readStream\
    .option("header","false")\
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("/stream")

    lines.createOrReplaceTempView("table")

    top = spark.sql("SELECT name, Followers/Friends as FRRatio FROM table WHERE Friends != 0 GROUP BY Name, FRRatio ORDER BY FRRatio DESC LIMIT 1")

    # Start running the query that prints the running counts to the console
    query = top\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    query.awaitTermination(100)
    query.stop()