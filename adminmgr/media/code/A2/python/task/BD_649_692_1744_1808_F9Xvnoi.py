import re
import sys
from operator import add
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from IPython.display import display 

# Initialize the spark context.
spark = SparkSession\
    .builder\
    .appName("BowlersRank")\
    .getOrCreate()

# File location and type
file_location = sys.argv[1]
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# Define column names

thisSchema = StructType().add("Batsman", StringType(), True).add("Bowler", StringType(), True).add("Wickets", IntegerType(), True).add("Deliveries", IntegerType(), True)
 
# Create a data frame with data from csv file
df = spark.read.format(file_type).schema(thisSchema).option("useHeader", "true") \
  .option("sep", delimiter) \
  .load(file_location)

#print(df)

# define variables
numberOfIterations = int(sys.argv[2])
dampingValue = int(sys.argv[3])
dampingValue = dampingValue/100

#Add bowler average to data frame
df = df.withColumn("bowlAvg", col("Wickets") / col("Deliveries"))
df.orderBy(df.Bowler, df.bowlAvg, df.Batsman, ascending=False).show()

# Get unique bowlers, sum of their bowling averages, and number of batsman out on their bowling

df_overallBolwerAvg = df.groupBy(df.Bowler).agg(sum("bowlAvg").alias("sumOfAvg"),count("*").alias("refCount")).orderBy(asc("Bowler"), "sumOfAvg")
df_overallBolwerAvg.show()

# Get the merged data frame with bowler, bastman, sumOfAvg, refCount etc.
df_bowlerRankData = df.join(df_overallBolwerAvg, on = "Bowler", how="left").select([df.Bowler, df.Batsman, when(df_overallBolwerAvg.sumOfAvg < 1, 1).otherwise(df_overallBolwerAvg.sumOfAvg).alias("sumOfAvg"), "refCount"]).orderBy(asc("Bowler"), asc("Batsman"))
df_bowlerRankData.show()

# If bowling average is less than 1 then substitue it by 1 else let original be there

def computeContribs(urls, rank):
    """Calculates player's contributions to the rank of other players."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a players pair string into players pair."""
    parts = re.split(',', urls)
    return parts[0], parts[1]

# Read players and strike rate from data file
lines = df_bowlerRankData.rdd.map(lambda r: r["Batsman"] + "," + r["Bowler"] + "," + str(r["sumOfAvg"]))
# print(lines.collect())


# Loads all players and initialize their neighbors.
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
# print(links.collect())

# Loads all players and their ranks
df_bowlersRankList = df_bowlerRankData.select("Bowler", "sumOfAvg").distinct()
ranks = df_bowlersRankList.rdd.map(lambda r: (r["Bowler"], r["sumOfAvg"]))
# print(ranks.collect())


# Get the first player in the list and corresponding rank. This will be used for comparing the result for rank conversion
prevFirstPlayer = ranks.first()
prevPlayerName = prevFirstPlayer[0]
prevPlayerRank = "%.4f" % prevFirstPlayer[1]
# print(prevPlayerName + "," + str(prevPlayerRank))

# Find the latest rank of the player
thisPlayerName = prevPlayerName
thisPlayerRankDF = ranks.toDF(["Bowler", "Rank"])
thisPlayerRankList = thisPlayerRankDF.filter(thisPlayerRankDF.Bowler == prevPlayerName).rdd.map(lambda x: x[1]).collect()
thisPlayerRank = thisPlayerRankList[0]
# print("Prev Player: " + prevPlayerName + ", Prev Rank: " + str(prevPlayerRank) + ", This Player: " + thisPlayerName + ", this Rank: " + str(thisPlayerRank))

# check if call is for convergence or limit to iterations
thisPlayerName = prevPlayerName

# Call for convergence
if (numberOfIterations == 0):
  # Calculates and updates Player ranks continuously using PageRank algorithm until convergence
  while (True):
      # increment loop value
      numberOfIterations = numberOfIterations + 1
      prevPlayerRank = thisPlayerRank
      
      # Calculates player's contributions to the rank of other players
      contribs = links.join(ranks).flatMap(
          lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
      
      # Re-calculates player ranks based on neighbor contributions.
      ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * dampingValue + (1 - dampingValue))
      
      thisPlayerRankDF = ranks.toDF(["Bowler", "Rank"])
      thisPlayerRankList = thisPlayerRankDF.filter(thisPlayerRankDF.Bowler == prevPlayerName).rdd.map(lambda x: x[1]).collect()
      thisPlayerRank = "%.4f" % thisPlayerRankList[0]
      
      # Print number of iterations
      # print("Number of iterations: " + str(numberOfIterations) + ", prevRank: " + prevPlayerRank + ", thisRank: " + thisPlayerRank)
      
      if ((float(prevPlayerRank) - float(thisPlayerRank)) == 0.0):
        break
else:
  # Calculates and updates Player ranks continuously using PageRank algorithm for number of iterations passed as parameter
  for iteration in range(int(numberOfIterations)):
      # Calculates player's contributions to the rank of other players
      contribs = links.join(ranks).flatMap(
          lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
      
      # Re-calculates player ranks based on neighbor contributions.
      ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * dampingValue + (1 - dampingValue))

# print(ranks.collect())

# Print number of iterations
# print("Number of iterations: " + str(numberOfIterations) + ", prevRank: " + prevPlayerRank + ", thisRank: " + thisPlayerRank)

# Collects all player ranks and dump them to console.
for (link, rank) in ranks.collect():
    print("%s,%s" % (link, rank))
