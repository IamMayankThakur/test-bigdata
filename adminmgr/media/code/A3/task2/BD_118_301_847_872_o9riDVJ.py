from pyspark.sql.types import TimestampType, StringType, StructType, StructField, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

import findspark
findspark.init()
spark = SparkSession\
        .builder\
        .appName("HashTagCount")\
        .getOrCreate()

inputPath = "hdfs://localhost:9000/stream/"
#inputPath = "/home/anagha/assignment_3/"

schema = StructType(
  [StructField("ID", StringType()),
        StructField("Lang", StringType()),
        StructField("Date", TimestampType()),
        StructField("Source", StringType()),
        StructField("len", IntegerType()),
        StructField("Likes", IntegerType()),
        StructField("RTs", IntegerType()),
        StructField("Hashtags", StringType()),
        StructField("UserMentionNames", StringType()),
        StructField("UserMentionID", StringType()),
        StructField("Name", StringType()),
        StructField("Place", StringType()),
        StructField("Followers", IntegerType()),
        StructField("Friends", IntegerType())
        ])

inputDF = (
 spark \
    .readStream \
    .option("sep", ";") \
    .option('maxFilesPerTrigger', 1)\
    .schema(schema) \
    .csv(inputPath)
)


spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small



inputDF.createOrReplaceTempView("popular")

person = spark.sql("select Name name, (Followers/Friends) as FRRatio from popular group by name, FRRatio order by FRRatio desc limit 1")



#words.groupBy("word").createOrReplaceTempView("hashtag_count")
#max_hashtag = spark.sql("select word, max(my_count) from hashtag_count group by word")
query = (
  person
    .writeStream
    .format("console")        # memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("counts")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
) 

query.awaitTermination(100)
query.stop()
spark.stop()
#https://hackersandslackers.com/structured-streaming-in-pyspark
#https://spark.apache.org/docs/latest/sql-reference.html
#https://spark.apache.org/docs/2.3.3/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets
#https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html


