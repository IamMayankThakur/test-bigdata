from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
'''#import spark.implicits._
#importing pyspark modules'''

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
#initializing spark context

#getting the coloumn values to acess them
coloumnName = variable_int StructType().add("identity", "string").add("encoding", "string").add("timestamp", "string").add("name_get", "string").add("length_x", "string").add("num_likes", "string").add("ran_var", "string").add("hashtag_number", "string").add("username", "string").add("userid", "string").add("t_name", "string").add("location", "string").add("num_followers", "integer").add("num_friends", "integer")
csvDF = spark\
  .readStream\
  .option("sep", ";")\
  .schema(coloumnName)\      #Specify schema of the csv files
  .csv("hdfs//localhost:9000/stream/") 
  
ratios = csvDF.select("t_name",(col("num_followers")/col("num_friends")).alias("FRRatio")) 
variable_int = ratios.groupBy("t_name","FRRatio").count()
sorted_variable_int = variable_int.orderBy(desc"FRRratio").select("t_name","FRRratio")
var = sorted_variable_int.limit(1)  #sort the values to get highest
#Todo:sort andprint highest

#idk what this means
query=var \
  .writeStream\
  .outputMode("complete")\
  .format("console")\
  .start()

#spark.sql("select * from aggregates").show(n=1)
query.awaitTermination(60)
query.stop()
  
  
