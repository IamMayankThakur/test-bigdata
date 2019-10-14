from __future__ import print_function

import re
import sys
from operator import add
from operator import sub

from pyspark.sql import SparkSession

# This function computes the weighted links for each player
def computeContribs(players, rank):
    num_players = len(players)
    for url in players:
        yield (url, rank / num_players)

# returns the links from batsman to bowler 
def parseNeighbors(players):
    parts = players.split(',')
    return parts[0], parts[1]

# returns the tuple of (bowler,bowling average)
def splitting(line):
	a,b,c,d=line.split(",")
	return (b,int(c)/int(d))   #return ((a,b),int(c)/int(d))

# if the difference between the elements of two RDDs is less than 0.0001 then, return 0 else return 1	
def subt(a,b):
	if(abs(a-b)<0.0001):
		return 0
	else:
		return 1
	
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
    # read the lines from the dataset
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    # compute the links and group by player name
    links = lines.map(lambda players: parseNeighbors(players)).distinct().groupByKey().cache()
    # compute the bowling averages
    avg=lines.map(lambda players:splitting(players)).distinct()
    final_avg=avg.map(lambda players:(players[0],players[1])).reduceByKey(add)
    # bowling average is given a value of 1 or the calculated average whichever is greater
    ranks = final_avg.map(lambda player_neighbors: (player_neighbors[0], max(1,round(player_neighbors[1],12))))
    ex=ranks
    x=1
    # checking if the first arguement is zero, which indicates that the loop must run till convergence
    if(int(sys.argv[2])==0):
    # checking if the second arguement is zero, which indicates that default weights must be considered.
    	if(int(sys.argv[3])==0):
    		while(x==1):
                        #compute the weighted links(importance) for each node
    			contribs = links.join(ranks).flatMap(lambda players_rank: computeContribs(players_rank[1][0], players_rank[1][1]))
    			#compute the weighted ranks
    			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
    			# execk the difference between the ranks in the revious iterations to the present one
    			diff=ex.join(ranks).map(lambda a:subt(a[1][0],a[1][1]))
			#If the sum of all the difference is zero, then the algorithm has reaexed convergence
    			val= diff.reduce(lambda a,b:a+b)
    			if(val==0):
    				x=0
    				break
    			ex=ranks
    	else:
		# the weights are given as the second arguement
    		s=int(sys.argv[3])
    		while(x==1):
    			contribs = links.join(ranks).flatMap(lambda players_rank: computeContribs(players_rank[1][0], players_rank[1][1]))
    			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (s/100) + (100-s)/100)
    			diff=ex.join(ranks).map(lambda a:subt(a[1][0],a[1][1]))
    			val= diff.reduce(lambda a,b:a+b)
    			if(val==0):
    				x=0
    				break
    			ex=ranks
    # The number of iterations are specified
    else:
    	if(int(sys.argv[3])==0):
    		for iteration in range(int(sys.argv[2])):
        		contribs = links.join(ranks).flatMap(lambda players_rank: computeContribs(players_rank[1][0], players_rank[1][1]))
        		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
    	else:
        	s=int(sys.argv[3])
        	for iteration in range(int(sys.argv[2])):
        		contribs = links.join(ranks).flatMap(lambda players_rank: computeContribs(players_rank[1][0],players_rank[1][1]))
        		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (s/100) + (100-s)/100)
    # sort the names alphabetically
    rankst=ranks.sortBy(lambda s:s[0],ascending=True)
    # sorts the runs in descending order
    final_ranks=rankst.sortBy(lambda x:x[1],ascending=False)
    for (link, rank) in final_ranks.collect():
    	print("%s,%s" % (link, round(rank,12))) # rounded upto 12 decimal places
    spark.stop()
