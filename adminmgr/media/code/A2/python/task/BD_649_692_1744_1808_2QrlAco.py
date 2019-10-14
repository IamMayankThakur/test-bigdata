import re
import sys
from operator import add
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import *
import gc

def rdd_iterate(rdd, chunk_size=10):
    indexed_rows = rdd.zipWithIndex().cache()
    count = indexed_rows.count()
    # print("Will iterate through RDD of count {}".format(count))
    start = 0
    end = start + chunk_size
    while start < count:
        # print("Grabbing new chunk: start = {}, end = {}".format(start, end))
        chunk = indexed_rows.filter(lambda r: r[1] >= start and r[1] < end).collect()
        for row in chunk:
            yield row[0]
        start = end
        end = start + chunk_size
        
        
def computeContribs(urls, rank):
    """Calculates player's contributions to the rank of other players."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a players pair string into players pair."""
    parts = re.split(',', urls)
    return parts[0], parts[1]
	

spark = SparkSession\
    .builder\
    .appName("BowlersRank")\
    .getOrCreate()

# File location and type
file_location = sys.argv[1]    # Replace the hardcoded file with argument sys.argv[0]
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Add/change default names to the column
df = df.select(col("_c0").alias("Batsman"), col("_c1").alias("Bowler"),col("_c2").alias("Wickets"),col("_c3").alias("Deliveries"))
#display(df)

# define variables
numberOfIterations = int(sys.argv[2])     # Replace the hardcoded number of iterations with argument sys.argv[1]
dampingValue = int(sys.argv[3])          # Replace the hardcoded number of damp value with argument sys.argv[2]
if dampingValue == 0:
  # This was .8 
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
df_overallBolwerAvg = df.join(df_overallBolwerAvg, on = "Bowler", how="left").select([df.Bowler, df.Batsman, when(df_overallBolwerAvg.sumOfAvg < 1, 1).otherwise(df_overallBolwerAvg.sumOfAvg).alias("sumOfAvg"), "refCount"]).orderBy(asc("Bowler"), asc("Batsman"))
# df_overallBolwerAvg.show()

df.unpersist()
del df
gc.collect()

# Cmd 5
# If bowling average is less than 1 then substitue it by 1 else let original be there

# Cmd 6	
# Read players and strike rate from data file
lines = df_overallBolwerAvg.rdd.map(lambda r: r["Batsman"] + "," + r["Bowler"] + "," + str(r["sumOfAvg"]))
# print(lines.collect())

# Cmd 7
# Loads all players and initialize their neighbors.
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
# print(links.collect())

lines.unpersist()
del lines
gc.collect()

# Cmd 8
# Loads all players and their ranks
df_overallBolwerAvg = df_overallBolwerAvg.select("Bowler", "sumOfAvg").distinct()
ranks = df_overallBolwerAvg.rdd.map(lambda r: (r["Bowler"], r["sumOfAvg"]))
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
thisPlayerRankList = rdd_iterate(thisPlayerRankDF.filter(thisPlayerRankDF.Bowler == prevPlayerName).rdd.map(lambda x: x[1]))
thisPlayerRank = thisPlayerRankList
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
      
      contribs.unpersist()
      del contribs
      gc.collect()
      
      thisPlayerRankDF = ranks.toDF(["Bowler", "Rank"])
      thisPlayerRankList = rdd_iterate(thisPlayerRankDF.filter(thisPlayerRankDF.Bowler == prevPlayerName).rdd.map(lambda x: x[1]))
      thisPlayerRank = "%.4f" % thisPlayerRankList
      
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
      
      contribs.unpersist()
      del contribs
      gc.collect()

# print(ranks.collect())

# Cmd 12
# Print number of iterations
# print("-------------- Number of iterations: " + str(numberOfIterations) + ", prevRank: " + str(prevPlayerRank) + ", thisRank: " + str(thisPlayerRank))

# Cmd 13
# Collects all player ranks and dump them to console.
for (link, rank) in rdd_iterate(ranks.sortBy(lambda a: a[1], False)):
    print("%s, %s" % (link, rank))