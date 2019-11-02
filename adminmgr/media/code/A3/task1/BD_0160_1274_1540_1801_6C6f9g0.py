from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import split, explode
import sys

def _get_instance_usage_schema():
        """get instance usage schema."""

        # Initialize columns for all string fields
        columns = ["ID","language","Date","source","len","likes","RTs","Hashtags","Usernames","Userid","name","Place","followers","friends"]

        columns_struct_fields = [StructField(field_name, StringType(), True)
                                 for field_name in columns]

        # Add columns for non-string fields
        '''columns_struct_fields.append(StructField("firstrecord_timestamp_unix",
                                                 DoubleType(), True))
        columns_struct_fields.append(StructField("lastrecord_timestamp_unix",
                                                 DoubleType(), True))
        columns_struct_fields.append(StructField("quantity",
                                                 DoubleType(), True))
        columns_struct_fields.append(StructField("record_count",
                                                 DoubleType(), True))

        columns_struct_fields.append(StructField("processing_meta",
                                                 MapType(StringType(),
                                                         StringType(),
                                                         True),
                                                 True))'''
        schema = StructType(columns_struct_fields)

        return schema


inputPath='hdfs://localhost:9000/stream/'

# Initialize the spark context.
spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

userSchema = _get_instance_usage_schema()

dfCSV = spark.readStream.option("sep", ";").option("header", "false").schema(userSchema).csv(inputPath)

#tweet=dfCSV.writeStream.foreach(lambda w:(w.split(';')[0],1)).format("console").start()
#count=tweet.reduceByKey(lambda x,y:x+y).toDF()
#query = tweet.writeStream.format("console").start()
#tweet=dfCSV.select("Hashtags")
tweet=dfCSV.select(explode(split(dfCSV.Hashtags,",")).alias("Hashtags"))
hashtag_count=tweet.groupBy("Hashtags").count().orderBy("count",ascending=False).where("Hashtags != ''")
common_hashtag=hashtag_count.select("Hashtags","count").limit(5)
query = common_hashtag.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(100)
query.stop()
