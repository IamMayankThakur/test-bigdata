from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import split, explode, col
import pyspark.sql.functions as f

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
tweet=dfCSV.select("name","followers","friends")
tweet=tweet.withColumn("followers",tweet["followers"].cast(DoubleType()))
tweet=tweet.withColumn("friends",tweet["friends"].cast(DoubleType()))
popular=tweet.withColumn("ratio",col("followers")/col("friends")).drop("followers","friends")
popular_ratio=popular.groupBy("name").agg(f.max("ratio").alias("FRRatio"))
#popular_user=popular.select("name","ratio").where(popular.ratio == popular_ratio.select("max(ratio)"))
popular_user=popular_ratio.select("name","FRRatio").orderBy("FRRatio",ascending=False).limit(1)
query = popular_user.writeStream.outputMode("complete").format("console").start()
query.awaitTermination(100)
query.stop()
