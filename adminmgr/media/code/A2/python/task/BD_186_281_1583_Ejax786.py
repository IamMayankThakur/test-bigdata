# Big data assignment-2 [UE17CS313] Sem-5, PES University, 2019.
'''
Ranking bowlers from the IPL using the PageRank algorithm.
Each player is equivalent to a page (as per the PageRank algorithm)
Initial rank of each player is max(sum(bowling_average_against_all_players), 1)
Links are between every batsman-bowler pair as given in the dataset
'''

from __future__ import print_function
import sys
import re
from operator import add
from pyspark.sql import SparkSession

def splitNeighbors(x):
	y = x.split(',')
	return y[0],y[1]	#x[0] -> batsman & x[1] -> bowler

def findInitialRanks(x):
	y = x.split(',')
	rank = float(float(y[2])/float(y[3]))
	return y[1], rank

def computeContribs(player_list,rank):
	num = len(player_list)
	
	for player in player_list:
		yield (player,float(rank/num))

if __name__ == "__main__":

	spark = SparkSession.builder.appName("pagerank").getOrCreate()
	filename = sys.argv[1]	#input dataset

	lines = spark.read.text(filename).rdd.map(lambda r: r[0])

	# every bowler with the set of players who faced him ***links go from batsman to bowler***
	links = lines.map(lambda x: splitNeighbors(x)).groupByKey().cache()

	initial_ranks = lines.map(lambda x: findInitialRanks(x)).reduceByKey(add).cache()

	ranks = initial_ranks.map(lambda x: (x[0],1) if (x[1] < 1) else (x[0],x[1]))
	old_ranks = ranks

	# Compute new ranks

	convergence = False	#convergence condition
	c = 0			#iteration count
	rank_weight = 0.80
	default_weight = 0.20
	#weights
	w = int(sys.argv[3])
	if ( w != 0):
		rank_weight =  w/100
		default_weight = 1 - rank_weight

	if(int(sys.argv[2]) != 0):	#iterative
		for i in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(lambda x: computeContribs(x[1][0],x[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * rank_weight + default_weight)
	else:
		while(not convergence):	#convergence
			c = c+1
			#print("\n\n\n",c,"\n\n\n")
			contribs = links.join(ranks).flatMap(lambda x: computeContribs(x[1][0],x[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * rank_weight + default_weight)

			temp = old_ranks.collect()
			
			# sort new and old independently
			ranks = ranks.sortBy(lambda x: x[0], True)			
			old_ranks = old_ranks.sortBy(lambda x: x[0],True)
			
			for i,j in zip(old_ranks.collect(),ranks.collect()):
				if(abs(float(j[1])-float(i[1]))>=0.0001):
					convergence = False
					break
				else:
					convergence = True
			old_ranks = ranks
	
	ranks=ranks.sortBy(lambda x:x[0],True)

	ranks=ranks.sortBy(lambda x:x[1],False)

	for (player, rank) in ranks.collect():
	        print("%s,%.12f" % (player,float(rank)))

	spark.stop()
