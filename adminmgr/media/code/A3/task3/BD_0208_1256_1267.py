# task 1 subtask 2

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import *

if __name__ == "__main__":
	spark = SparkSession.builder\
	.appName("SparkStream").getOrCreate()

    userSchema = StructType().add("ID", "string").add("Lang", "string") \
    .add("Date","string").add ("Source","string").add("len","integer") \
    .add("Likes","integer").add("RTs","integer").add("Hashtags","string") \
    .add("UserMentionNames","string").add("UserMentionID","string") \
    .add("Name","string").add("Place","string").add("Followers","integer") \
    .add("Friends","integer")


    df1 = spark.readStream \
    	.option("sep", ";").schema(userSchema) \
    	.csv("/stream")

	df1.createOrReplaceTempView("table")

    q = "select ,count() from table group by ID,Lang,Date,Source,len,Likes,RTs,Hashtags,UserMentionNames,UserMentionID,Name,Place,Followers,Friends"
	df=spark.sql(q)

	df.createOrReplaceTempView("table2")

    q1 = "SELECT Name as name,Followers/Friends as FRRatio FROM table2 ORDER BY FRRatio DESC LIMIT 1"
	df1 = spark.sql(q1)

	query = df1.writeStream \
        .outputMode('complete') \
            .format("console").start()

	query.awaitTermination(100)
	query.stop()