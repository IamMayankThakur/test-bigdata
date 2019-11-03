#assignment 3
from pyspark.sql import SparkSession
#assignment
from pyspark.sql.types import StructType
#sparkcode
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
#sublimetext
dfCSV = spark.readStream.option("sep", ";").option("header", "false").schema(userSchema).csv("/stream")
#reading
dfCSV.createOrReplaceTempView("Bigdata")
#view
total = spark.sql("select Hashtags.count(Followers/Friends) as FRratio from Bigdata order by count desc limit 10")
#total
q = total.writeStream.outputMode("complete").format("console")
#write
q.start()
#start
q.awaitTermination(60)
#end
spark.stop()