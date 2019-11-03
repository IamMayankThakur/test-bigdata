from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
sc = SparkContext.getOrCreate()

sqlContext = SQLContext(sc)
userSchema = StructType().add("ID", "integer").add("Lang", "string").add("Date", "string").add("Source", "string").add("Len", "integer").add("Likes", "integer").add("Rts", "integer").add("Hashtags", "string").add("Usernames", "string").add("UserID", "string").add("name", "string").add("place", "string").add("followers", "integer").add("friends", "integer")

csvDF = spark \
    .readStream \
    .option("sep",';') \
    .schema(userSchema) \
    .csv("/stream") 

querydf = csvDF.select("Name","followers","friends")
newdf = querydf.withColumn("ratio",querydf.followers/querydf.friends)
df2 = newdf.groupBy("Name").agg(F.max("ratio")).orderBy(F.desc('max(ratio)')).limit(1)
df2 = df2.select(col("Name").alias("name"), col("max(ratio)").alias("FRRatio"))

query = df2 \
	.writeStream \
	.format("console") \
	.outputMode("complete") \
	.start()
query.awaitTermination(100)
query.stop()


