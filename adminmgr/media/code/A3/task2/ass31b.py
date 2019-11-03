import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType,StringType,IntegerType,StructField
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
#directory = "hdfs://localhost:9000/stream"
directory = "/home/hduser/Desktop/Assignment3/Input"

spark = SparkSession.builder.appName("Most common hashtag").getOrCreate()

tweetSchema = StructType().add("id","string").add("lang","string").add("date","string").add("source","string").add("len","integer").add("likes","integer").add("rts","integer").add("Hashtags","string").add("usermentionnames","string").add("usermentionid","string").add("name","string").add("place","string").add("followers","integer").add("friends","integer")

tweetdf = spark.readStream.option("sep",";").schema(tweetSchema).csv(directory)

base = tweetdf.select("name","followers","friends").where("friends != 0").withColumn("FRRatio",tweetdf.followers/tweetdf.friends)

ratio = base.select("name","FRRatio")
ratio.createOrReplaceTempView("ratio")

#maxval = ratio.groupBy().max('FRRatio')
#maxval = base.agg(F.max(base.FRRatio).alias("maxratio"))
#maxval.createOrReplaceTempView("maxval")

#result = ratio.select("name","FRRatio").withColumn("max ratio",maxval.maxratio)
#result = base.select("name","FRRatio").withColumn("max ratio",newval).filter("max ratio"=="FRRatio").drop("FRRatio").withColumnRenamed("max ratio","FRRatio")

command = spark.sql("select name, max(FRRatio) as FRRatio from ratio group by name order by max(FRRatio) desc limit 1")
query = command.writeStream.format("console").outputMode("complete").start()

query.awaitTermination(100)
query.stop()
