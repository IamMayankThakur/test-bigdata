from pyspark.sql import SparkSession
#from pyspark import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import *
if __name__ == "__main__":
	spark = SparkSession\
	.builder\
	.appName("PythonPageRank")\
	.getOrCreate()
	userSchema = StructType().add("ID", "string").add("Lang", "string").add("Date","string").add ("Source","string").add("len","integer").add("Likes","integer").add("RTs","integer").add("Hashtags","string").add("UserMentionNames","string").add("UserMentionID","string").add("name","string").add("Place","string").add("Followers","integer").add("Friends","integer")
	csvDF = spark \
    	.readStream \
    	.option("sep", ";") \
    	.schema(userSchema) \
    	.csv("/home/jnanesh/Datasets")
	csvDF.createOrReplaceTempView("tweets")
	dataDF=csvDF.select(csvDF["name"],csvDF["Followers"],csvDF["Friends"])
	#dataDF.createOrReplaceTempView("table1")
	wordCounts = dataDF.groupBy('name',"Followers","Friends").count()
	wordCounts.createOrReplaceTempView("table")
	#wordCounts.createOrReplaceTempView("Hashtags")
	sqlDF=spark.sql("SELECT name,CAST(Followers AS float) / CAST(Friends AS float) AS FRRatio FROM table ORDER BY FRRatio DESC LIMIT 1")
	'''sqlDF=spark.sql("SELECT DISTINCT Name,CAST(Followers AS float) / CAST(Friends AS float) AS ratio FROM tweets")
	
	#sqlDF=csvDF.select(csvDF["Name"],csvDF["Followers"]/csvDF["Friends"] as "ratio").orderBy('(Followers / Friends)', ascending=False)
	sqlDF.createOrReplaceTempView("users")
	maxquery=spark.sql("SELECT MAX(ratio) FROM users")
	maxquery.createOrReplaceTempView("maxuser")
	result=spark.sql("SELECT Name,ratio FROM users WHERE ratio IN maxuser")'''
	#sortedwords = spark.sql("SELECT word FROM Hashtags ORDER BY count DESC LIMIT 1")'''
	query=sqlDF.writeStream.outputMode('complete').format("console").start()
	#sleep(10)
	query.awaitTermination(100)
	#csvDF.show()
	query.stop()
