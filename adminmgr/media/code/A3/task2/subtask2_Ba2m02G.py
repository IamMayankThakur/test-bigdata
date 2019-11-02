from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

def printoutput(data):
	ans=[]
	count=0
	obtained=False
	print(data[0]['name'])

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

# Define schema of the csv
userSchema = StructType().add("ID", "string").add("language","string").add("Date","date").add("source","string").add("len","integer").add("likes","integer").add("RTs","integer").add("Hashtags","string").add("Usernames","string").add("Userid","string").add("name","string").add("Place","string").add("followers","integer").add("friends","integer")

# Read CSV files from set path
dfCSV = spark.readStream.option("sep", ";").schema(userSchema).csv('/stream')

dfCSV.createOrReplaceTempView("salary")

totalSalary = spark.sql("select name,max(followers/friends) as FRRatio from salary where friends>0 group by name order by FRRatio desc limit 1")

query = totalSalary.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(10)
query.stop()

# query = totalSalary.writeStream.outputMode("complete").format("memory").queryName("sample").start()
# query.awaitTermination(10)
# data=spark.sql("Select * from sample").collect()
# printoutput(data)
# query.awaitTermination(10)
# data=spark.sql("Select * from sample").collect()
# printoutput(data)
# query.awaitTermination(10)
# data=spark.sql("Select * from sample").collect()
# printoutput(data)
# query.awaitTermination(10)
# data=spark.sql("Select * from sample").collect()
# printoutput(data)
# query.awaitTermination(10)
# data=spark.sql("Select * from sample").collect()
# printoutput(data)
# query.stop()