from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Most popular user").getOrCreate()


userSchema = StructType().add("A", "string") \
		.add("B", "string") \
		.add("C", "string") \
		.add("D", "string") \
		.add("E", "string") \
		.add("F", "string") \
		.add("G", "string") \
		.add("H", "string") \
		.add("I", "string") \
		.add("J", "string") \
		.add("K", "string") \
		.add("L", "string") \
		.add("M", "string") \
		.add("N", "string")

csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream/")



df1 = csvDF.select("K", "M", "N").withColumn("ratio", (csvDF.M/ csvDF.N))

pop = df1.groupBy("K").agg(F.max("ratio")).orderBy("max(ratio)", ascending = False).withColumnRenamed("max(ratio)", "FRRatio").withColumnRenamed("K", "name").limit(1)

pop1 = pop.writeStream.outputMode("complete").format("console").start()

pop1.awaitTermination(100)
pop1.stop()
spark.stop()
