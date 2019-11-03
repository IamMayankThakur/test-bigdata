from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredHashtagCount") \
    .getOrCreate()

fileSchema = StructType().add("ID", "string").add("language", "string").add("Date","string").add("source","string").add("len","integer").add("likes","integer").add("RTs","integer").add("Hashtags","string").add("Usernames","string").add("Userid","string").add("name","string").add("place","string").add("followers","integer").add("friends","integer")
csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(fileSchema) \
    .csv("/stream")  # Equivalent to format("csv").load("/path/to/directory")


hashtags = csvDF.select(
   explode(
       split(csvDF.Hashtags, ",")
   ).alias("hashtag")
)

hashtags_count = hashtags.groupBy("hashtag").count()

hashtags_count.createOrReplaceTempView("hashtag_sql_table")
final_hashtag_count = spark.sql("select hashtag as Hashtags,count as Count from hashtag_sql_table order by count desc limit 5 ")

#select TOP(5) hashtag  from hashtag_sql_table GROUP BY hashtag ORDER BY Count DESC

#select hashtag as hs from hashtag_sql_table order by count desc limit 5 

query = final_hashtag_count \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

