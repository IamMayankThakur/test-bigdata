from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from pyspark.sql import window
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("Most popular user").getOrCreate()


userSchema = StructType().add("1", "string") \
		.add("2", "string") \
		.add("3", "string") \
		.add("4", "string") \
		.add("5", "string") \
		.add("6", "string") \
		.add("7", "string") \
		.add("c8", "string") \
		.add("9", "string") \
		.add("10", "string") \
		.add("11", "string") \
		.add("12", "string") \
		.add("c13", "string") \
		.add("c14", "string")

csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream/")



df1 = csvDF.select("11", "c13", "c14").withColumn("ratio", (csvDF.c13/ csvDF.c14))

pop = df1.groupBy("11").agg(F.max("ratio")).orderBy("max(ratio)", ascending = False).withColumnRenamed("max(ratio)", "FRRatio").withColumnRenamed("11", "name").limit(1)

pop1 = pop.writeStream.outputMode("complete").format("console").start()

pop1.awaitTermination(100)
pop1.stop()
spark.stop()
