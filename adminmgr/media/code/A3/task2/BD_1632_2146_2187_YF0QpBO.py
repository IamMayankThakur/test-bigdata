import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark = SparkSession \
    .builder \
    .appName("Bigdata") \
    .getOrCreate()

userSchema = StructType().add("ID","string").add("Lang","string",).add("Date","string").add("Source","string").add("len","integer").add("Likes","integer").add("RTs","integer").add("Hashtags","string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","integer").add("Friends","integer")
'''csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("/stream1")
'''

dfCSV = spark.readStream.option("sep", ";").option("header", "false").schema(userSchema).csv("/stream")
dfCSV.createOrReplaceTempView("Bigdata")
total = spark.sql("select Name,max(Followers/Friends) as FRRatio from Bigdata group by Name order by FRRatio desc limit 1")
q = total.writeStream.outputMode("complete").format("console")

q.start().awaitTermination(60)
spark.stop()




