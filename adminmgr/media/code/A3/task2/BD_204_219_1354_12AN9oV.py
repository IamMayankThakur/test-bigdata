from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,count,max,struct
from pyspark.sql.types import StructType, IntegerType

spark=SparkSession \
	.builder \
	.appName("Task1_1") \
	.getOrCreate()

userSchema=StructType().add("ID","string").add("Lang","string").add("Date","string").add("Source","string").add("len","integer").add("likes","integer").add("RT","integer").add("Hash","string").add("UserN","string").add("UserID","string").add("name","string").add("Place","string").add("Follow","integer").add("Friend","integer")
csvdf=spark \
	.readStream \
	.option("sep",";") \
	.schema(userSchema) \
	.csv("hdfs://localhost:9000/stream")

#print(type(csvdf))

cols=csvdf.select("name","Friend","Follow")
cols=cols.withColumn("FRRatio",cols["Follow"].cast(IntegerType())/cols["Friend"].cast(IntegerType()))
cols1=cols.select("name","FRRatio")
popular=cols1.select(max(struct("FRRatio",*(x for x in cols1.columns if x!="FRRatio"))).alias("name"))

puser = popular.select("name.name","name.FRRatio")

#popular=popular.select("UserID","Ratio").orderBy("Ratio",ascending=False)

#.rdd.flatmap(lambda x:(x.split(','),1))
#print(type(tags))
#words = cols.select()
#wordcount=words.groupby("splithash").count()
#most=wordcount.select("splithash","count").orderBy("count",ascending=False).limit(1)

query=puser.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(60)
query.stop()
