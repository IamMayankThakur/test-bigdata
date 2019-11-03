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
    .csv("/input")
'''

dfCSV = spark.readStream.option("sep", ";").option("header", "false").schema(userSchema).csv("/stream")
dfCSV.createOrReplaceTempView("Bigdata")
total = spark.sql("select Hashtags from Bigdata")
q = total.writeStream.outputMode("complete").format("console")
q.start()
q.awaitTermination()