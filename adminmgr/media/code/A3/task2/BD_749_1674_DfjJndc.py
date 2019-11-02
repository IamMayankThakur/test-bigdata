import findspark
findspark.init()
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

df1=spark.sql("SELECT name ,followers / friends AS FRRatio FROM USER GROUP BY name,FRRatio ORDER BY FRRatio desc limit 1")




query =df1\
    .writeStream \
    .outputMode("complete")\
    .format("console") \
    .start()

query.awaitTermination(60)
query.stop()
