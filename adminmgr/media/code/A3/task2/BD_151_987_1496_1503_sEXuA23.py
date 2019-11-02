from pyspark.sql.types import StringType, StructType, StructField
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split,max
sc = SparkContext('local')
#sc.setLogLevel("OFF")
spark = SparkSession(sc)
# Path to our 20 JSON files
inputPath = "hdfs://localhost:9000/stream/"
#inputPath = "./stream/"
# Explicitly set schema
schema = StructType([ StructField("ID", StringType(), True),
                      StructField("Lang", StringType(), True),
                      StructField("Date", StringType(), True),
                      StructField("Source", StringType(), True),
                      StructField("Len", StringType(), True),
                      StructField("Likes", StringType(), True),
                      StructField("RTs", StringType(), True),
                      StructField("Hashtags", StringType(), True),
                      StructField("UserMentionNames", StringType(), True),
                      StructField("UserMentionID", StringType(), True),
                      StructField("name", StringType(), True),
                      StructField("Place", StringType(), True),
                      StructField("Followers", StringType(), True),
                      StructField("Friends", StringType(), True)])


inputDF = spark.readStream.schema(schema).option("delimiter",";").option("delimiter",";").option("maxFilesPerTrigger",1).csv(inputPath)




#query1 =inputDF.select(explode(split("Hashtags", ",")).alias("Hashtags")).groupBy("Hashtags").count().orderBy('count', ascending=False)

   

query2 = inputDF.withColumn("Ratio",inputDF.Followers/inputDF.Friends).groupBy('name').agg(max('Ratio').alias('FRRatio')).orderBy('FRRatio', ascending=False)


#query1.writeStream.outputMode("complete").format("console").option("numRows",5).start().awaitTermination(100)

    
query2.writeStream.outputMode("complete").format("console").option("numRows",1).start().awaitTermination(60)
#query2.stop()
#spark.stop()

