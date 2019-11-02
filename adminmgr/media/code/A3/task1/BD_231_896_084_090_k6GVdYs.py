from __future__ import print_function

import sys
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("BD_Assignment3_Task1_2")\
        .getOrCreate()

    schemaString = "ID, Lang, Date, Source, len, Likes, RTs, Hashtags, UserMentionNames, UserMentionID, Name, Place, Followers, Friends"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(", ")]
    userSchema = StructType(fields)

    lines = spark\
    .readStream\
    .option("header","false")\
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("/stream")

    hasht = lines.select("Hashtags").where("Hashtags is not null")

    hashs = hasht.select(
        explode(
            split(hasht.Hashtags,',')
        ).alias("Hashtags") 
    )
    
    top = hashs.groupBy("Hashtags").count()

    top_a = top.orderBy("count",ascending=False)

    top5 = top_a.limit(5)
    # Start running the query that prints the running counts to the console
    query = top5\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    query.awaitTermination(100)
    query.stop()