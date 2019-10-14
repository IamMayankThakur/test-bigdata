import re
import sys
from operator import add
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession\
    .builder\
    .appName("BowlersRank")\
    .getOrCreate()

# Cmd 1
# File location and type
file_location = sys.argv[1]     # Replace the hardcoded file with argument sys.argv[0]
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Add/change default names to the column
from pyspark.sql.functions import col
df = df.select(col("_c0").alias("Batsman"), col("_c1").alias("Bowler"),col("_c2").alias("Wickets"),col("_c3").alias("Deliveries"))
#display(df)

# define variables
numberOfIterations = int(sys.argv[2])     # Replace the hardcoded number of iterations with argument sys.argv[1]
dampingValue = .80          # Replace the hardcoded number of damp value with argument sys.argv[2]
if dampingValue == 0: 
  dampingValue = .2
else:
  dampingValue = (100 - dampingValue) / 100
  
  
# Cmd 2
#Add bowler average to data frame
df = df.withColumn("bowlAvg", col("Wickets") / col("Deliveries"))
# df.orderBy(df.Bowler, df.bowlAvg, df.Batsman, ascending=False).show()

# Cmd 3
# Get unique bowlers, sum of their bowling averages, and number of batsman out on their bowling
df_overallBolwerAvg = df.groupBy(df.Bowler).agg(sum("bowlAvg").alias("sumOfAvg"),count("*").alias("refCount")).orderBy(asc("Bowler"), "sumOfAvg")
# df_overallBolwerAvg.show()

# Cmd 4
# Get the merged data frame with bowler, bastman, sumOfAvg, refCount etc.
df_bowlerRankData = df.join(df_overallBolwerAvg, on = "Bowler", how="left").select([df.Bowler, df.Batsman, when(df_overallBolwerAvg.sumOfAvg < 1, 1).otherwise(df_overallBolwerAvg.sumOfAvg).alias("sumOfAvg"), "refCount"]).orderBy(asc("Bowler"), asc("Batsman"))
# df_bowlerRankData.show()

# Cmd 5
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
	

# Cmd 6	
# Read players and strike rate from data file
lines = df_bowlerRankData.rdd.map(lambda r: r["Batsman"] + "," + r["Bowler"] + "," + str(r["sumOfAvg"]))
# print(lines.collect())

# Cmd 7
# Loads all players and initialize their neighbors.
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
# print(links.collect())

# Cmd 8
# Loads all players and their ranks
df_bowlersRankList = df_bowlerRankData.select("Bowler", "sumOfAvg").distinct()
ranks = df_bowlersRankList.rdd.map(lambda r: (r["Bowler"], r["sumOfAvg"]))
# print(ranks.collect())

# Cmd 9
# Get the first player in the list and corresponding rank. This will be used for comparing the result for rank conversion
prevFirstPlayer = ranks.first()
prevPlayerName = prevFirstPlayer[0]
prevPlayerRank = "%.4f" % prevFirstPlayer[1]
# print(prevPlayerName + "," + str(prevPlayerRank))

# Cmd 10
# Find the latest rank of the player
thisPlayerName = prevPlayerName
thisPlayerRankDF = ranks.toDF(["Bowler", "Rank"])
thisPlayerRankList = thisPlayerRankDF.filter(thisPlayerRankDF.Bowler == prevPlayerName).rdd.map(lambda x: x[1]).collect()
thisPlayerRank = thisPlayerRankList[0]
# print("Prev Player: " + prevPlayerName + ", Prev Rank: " + str(prevPlayerRank) + ", This Player: " + thisPlayerName + ", this Rank: " + str(thisPlayerRank))

# Cmd 11
# check if call is for convergence or limit to iterations
#this = numberOfIterations    # 0 Remove line. It is only written for testing purpose.
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
      # print("-------------- Number of iterations: " + str(numberOfIterations) + ", prevRank: " + str(prevPlayerRank) + ", thisRank: " + str(thisPlayerRank))
      
      if ((float(prevPlayerRank) - float(thisPlayerRank)) == 0.0):
        break
else:
  # Calculates and updates Player ranks continuously using PageRank algorithm for number of iterations passed as parameter
  for iteration in range(int(numberOfIterations)):
      #sys.exit(0)
      # Calculates player's contributions to the rank of other players
      contribs = links.join(ranks).flatMap(
          lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
      
      # Re-calculates player ranks based on neighbor contributions.
      ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * dampingValue + (1 - dampingValue))

# print(ranks.collect())

# Cmd 12
# Print number of iterations
# print("-------------- Number of iterations: " + str(numberOfIterations) + ", prevRank: " + str(prevPlayerRank) + ", thisRank: " + str(thisPlayerRank))

# Cmd 13
# Collects all player ranks and dump them to console.
for (link, rank) in ranks.sortByKey().collect():
    print("%s, %s" % (link, rank))
