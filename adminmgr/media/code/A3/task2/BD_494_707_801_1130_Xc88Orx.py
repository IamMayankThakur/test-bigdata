import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

spark = SparkSession\
  .builder\
  .appName("Task1_2")\
  .getOrCreate()

userSchema = StructType().add("ID", "string").add("Lang", "string").add("Date","string").add("Source","string").add("len","integer").add("likes","string").add("RTs","string").add("Hashtags",
"string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","string").add("Friends","string")

csvDF = spark.readStream.option("sep", ";").schema(userSchema).csv("hdfs://localhost:9000/stream") 
ratios = csvDF.select("name",(col("Followers")/col("Friends")).alias("FRRatio")) 
new = ratios.groupBy("name","FRRatio").count()
sorted_new = new.orderBy("FRRatio",ascending=False).select("name","FRRatio")
var = sorted_new.limit(5)

query=var \
  .writeStream\
  .outputMode("complete")\
  .format("console")\
  .start()

query.awaitTermination(60)
query.stop()