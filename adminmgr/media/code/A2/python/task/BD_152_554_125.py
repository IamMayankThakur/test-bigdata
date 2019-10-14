from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession

def initialRank(player, ranks):
	x = 0
	for rank in ranks:
		x = x + rank
	return player, max(x, 1.0)
		
def computeContribs(players, rank):
    num_players = len(players)
    for player in players:
        yield (player, rank / num_players)

def parseNeighbors1(players):
    parts = players.split(',')
    return parts[0], parts[1]
    
def parseNeighbors2(players):
    parts = players.split(',')
    rate = float(int(parts[2])/int(parts[3]))
    return parts[0], rate

def checkConvergence(a, b):
        a_ = a.collect()
        b_ = b.collect()
        a_.sort(key = lambda x : x[0])
        b_.sort(key = lambda x : x[0])
        for x in range(len(a_)):
            i = float(a_[x][1])
            j = float(b_[x][1])
            if(((i - j) > 0 and (i - j) > 0.0001) or ((i - j) < 0 and (i - j) < -0.0001)):
                return 1
        return 0

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
        
    x = float(int(sys.argv[3])/100)
    if (x == 0):
        x = 0.8

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links1 = lines.map(lambda players: parseNeighbors1(players)).distinct().groupByKey().cache()
    links2 = lines.map(lambda players: parseNeighbors2(players)).distinct().groupByKey().cache()

    ranks = links2.map(lambda player_neighbors: initialRank(player_neighbors[0], player_neighbors[1]))

    contribs = links1.join(ranks).flatMap(lambda player_rank: computeContribs(player_rank[1][0], player_rank[1][1]))
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * x + (1-x))
    prev_ranks = ranks
    curr_ranks = prev_ranks
    
    if (int(sys.argv[2]) == 0):
        contribs = links1.join(curr_ranks).flatMap(lambda player_rank: computeContribs(player_rank[1][0], player_rank[1][1]))
        curr_ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * x + (1-x))
        while(checkConvergence(prev_ranks, curr_ranks)):
            contribs = links1.join(curr_ranks).flatMap(lambda player_rank: computeContribs(player_rank[1][0], player_rank[1][1]))
            prev_ranks = curr_ranks
            curr_ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * x + (1-x))
            
    else:
        for iteration in range(int(sys.argv[2]) - 1):
       	    contribs = links1.join(curr_ranks).flatMap(lambda player_rank: computeContribs(player_rank[1][0], player_rank[1][1]))
            curr_ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * x + (1-x))

    data = curr_ranks.sortBy(lambda players: (-players[1],players[0]))
    for (player, rank) in data.collect():
	    print("%s, %.12f" % (player, float(rank)))

    spark.stop()
