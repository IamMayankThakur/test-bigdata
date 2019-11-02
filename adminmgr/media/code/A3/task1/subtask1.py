from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
import time

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

# Define schema of the csv
userSchema = StructType().add("ID", "string").add("language","string").add("Date","date").add("source","string").add("len","integer").add("likes","integer").add("RTs","integer").add("Hashtags","string").add("Usernames","string").add("Userid","string").add("name","string").add("Place","string").add("followers","integer").add("friends","integer")

# Read CSV files from set path
dfCSV = spark.readStream.option("sep", ";").schema(userSchema).csv('/stream')

dfCSV.createOrReplaceTempView("tweets")
totalSalary = spark.sql("select Hashtags,count(Hashtags) as count from tweets group by Hashtags order by count(Hashtags) desc,Hashtags limit 5")

query = totalSalary.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(10)
query.stop()
