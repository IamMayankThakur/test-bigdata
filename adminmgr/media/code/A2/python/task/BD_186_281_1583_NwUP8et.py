# Big data assignment-2 [UE17CS313] Sem-5, PES University, 2019.
'''
Ranking bowlers from the IPL using the PageRank algorithm.
Each player is equivalent to a page (as per the PageRank algorithm)
Initial rank of each player is max(sum(bowling_average_against_all_players), 1)
Links are between every batsman-bowler pair as given in the dataset
'''

from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession
import sys



def computeContribs(batsman_list, current_rank):

	batsman_num = len(batsman_list)

	for n in batsman_list:
		yield(n, float(current_rank/batsman_num))


if __name__ == "__main__":

	spark = SparkSession.builder.appName("bowlerRank").getOrCreate()

	filename = sys.argv[1]	#input dataset

	lines = spark.read.text(filename).rdd.map(lambda r: r[0])

	# every bowler with the set of players who faced him ***links go from batsman to bowler***
	links = lines.map(lambda x: x.split(',')).map(lambda x: (x[0],x[1])).groupByKey()	#x[0] -> batsman & x[1] -> bowler

	# using max(sum(averages),1) for each bowler's inital rank
	initial_ranks = lines.map(lambda x: x.split(',')).map(lambda x: (x[1],float(int(x[2])/int(x[3])))).reduceByKey(add)

	ranks = initial_ranks.map(lambda x: (x[0],1.0) if (x[1] < 1.0) else (x[0],x[1]))

	old_ranks = ranks.sortByKey().collect()

	total = initial_ranks.count()

	# Compute new ranks

	convergence = False	# convergence condition
	c = 0			# iteration count

	calc_weight = 0.80

	# weight argument
	if(int(sys.argv[3]) != 0):
		calc_weight = float((int(sys.argv[3]))/100)						
											

	# For Iterative computation
	if(int(sys.argv[2]) != 0):

		for i in range(int(sys.argv[2])):

			contribs = links.join(ranks).flatMap(lambda x: computeContribs(x[1][0],x[1][1]))

			ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank*calc_weight) + (1-calc_weight))

	# For computation until convergence
	else:
		while( not convergence ):
			c = c + 1

			contribs = links.join(ranks).flatMap(lambda x: computeContribs(x[1][0],x[1][1]))

			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*calc_weight + (1-calc_weight))

			new_ranks = ranks.sortByKey().collect()

			for i in range(total):

				diff = abs(old_ranks[i][1] - new_ranks[i][1])

				if(diff < 0.0001):
					convergence = True
				else:
					convergence = False
					break

			old_ranks = new_ranks


	ranks = ranks.sortBy(lambda x: x[0],True)
	
	sorted_output = ranks.sortBy(lambda x: x[1],False).collect()

	# Print NEW RANKS
	for (bowler,rank) in sorted_output:
		#print(bowler,round(rank,12),sep=",")
		print(bowler,'%.12f'%rank,sep=",")		
	
	#print("\nNumber of Iterations: ",c)

	spark.stop()
