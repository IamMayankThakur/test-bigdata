import findspark
findspark.init()


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

spark = SparkSession\
  .builder\
  .appName("BD3")\
  .getOrCreate()
  
# Read all the csv files written atomically in a directory
userSchema = StructType().add("ID", "string").add("Lang", "string").add("Date","string").add("Source","string").add("len","integer").add("likes","string").add("RTs","string").add("Hashtags",
"string").add("UserMentionNames","string").add("UserMentionID","string").add("Name","string").add("Place","string").add("Followers","string").add("Friends","string")

csvDF = spark.readStream.option("sep", ";").schema(userSchema).csv("hdfs://localhost:9000/stream") 
  
ratios = csvDF.select("name",(col("Followers")/col("Friends")).alias("FRRatio")) 
new = ratios.groupBy("name","FRRatio").count()
sorted_new = new.orderBy("FRRatio",ascending=False).select("name","FRRatio")
var = sorted_new.limit(1)
# ========== DF with aggregation ==========
#val aggDF = df.groupBy("device").count()

#Print updated aggregations to console
query=var \
  .writeStream\
  .outputMode("complete")\
  .format("console")\
  .start()

#spark.sql("select * from aggregates").show(n=1)
query.awaitTermination(60)
query.stop()
  
  
