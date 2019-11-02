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


#csvDF.dropDuplicates(["name"])
csvDF.createOrReplaceTempView("USER")
df1=spark.sql("SELECT DISTINCT name ,followers,friends FROM USER GROUP BY name,followers,friends")

#streamingDf.dropDuplicates("guid")

df1.dropDuplicates(["name"])
df1.createOrReplaceTempView("USER1")

df=spark.sql("SELECT  name , followers / friends AS FRRatio FROM USER1 ORDER BY FRRatio desc  limit 10")



query =df\
    .writeStream \
    .outputMode("complete")\
    .format("console") \
    .start()

query.awaitTermination(60)
query.stop()
