from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkContext,SparkConf


spark = SparkSession \
    .builder \
    .appName("task1") \
    .getOrCreate()


userSchema = StructType().add("ID", "integer").add("lang","string").add("date","string").add("source","string").add("len",
"integer").add("likes","integer").add("RTs","integer").add("hashtags","string").add("usermentionnames","string").add("usermentionID",
"string").add("name","string").add("place","string").add("followers","integer").add("friends","integer")

csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream/")

words = csvDF.select(
   explode(
       split(csvDF.hashtags, ",")
   ).alias("Hashtags")
)

counts = words.groupBy("Hashtags").count()

counts.createOrReplaceTempView("temp_table")
#new=spark.sql("select hashtag,count from temp_table where count=max")

count_1=counts.orderBy(counts['count'].desc()).limit(5)
#h=count_1.select(count_1['Hashtags'])
#h2=h["hashtag"][0]


query =count_1 \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination(60)
query.stop()
