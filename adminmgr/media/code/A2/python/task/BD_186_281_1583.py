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
import findspark

findspark.init()


def computeContribs(merged_tuple):
	batsman_list = merged_tuple[1][0]	#list of batsman the bowler has bowled to
	current_rank = merged_tuple[1][1]	#the bowlers current rank
	batsman_num = len(batsman_list)

	for n in batsman_list:
		yield(n, current_rank/batsman_num)


if __name__ == "__main__":

	spark = SparkSession.builder.appName("bowlerRank").getOrCreate()

	filename = sys.argv[3]	#input dataset

	lines = spark.read.text(filename).rdd.map(lambda r: r[0])

	# every bowler with the set of players who faced him ***links go from batsman to bowler***
	links = lines.map(lambda x: x.split(',')).map(lambda x: (x[0],x[1])).groupByKey().cache()	#x[0] -> batsman & x[1] -> bowler

	# using max(sum(averages),1) for each bowler's inital rank
	#initial_ranks = lines.map(lambda x: x.split(',')).map(lambda x: (x[1],1) if ((int(x[2])/int(x[3])) < 1) else (x[1],int(x[2])/int(x[3])) ).reduceByKey(add)
	initial_ranks = lines.map(lambda x: x.split(',')).map(lambda x: (x[1],int(x[2])/int(x[3]))).reduceByKey(add)

	ranks = initial_ranks.map(lambda x: (x[0],1) if (x[1]<1) else (x[0],x[1]))

	old_ranks = ranks.sortByKey().collect()

	total = initial_ranks.count()

	# Print INITIAL RANK LIST and length
	#print("\n")
	#for (bowler,avg) in old_ranks:
		#print(bowler,avg)
	#print("\n")

	#print("\n Ranks Count:",total,"\n\n")

	links_output = links.collect()

	# Print INITIAL LINK LIST and length
	#print("\n")
	#for (bowler,l) in links_output:
		#print(bowler,list(l),"\n")
	#print("\n")

	#print("\n Link Count:",links.count(),"\n\n")


	# Compute new ranks

	convergence = False	# convergence condition
	c = 0			# iteration count

	calc_weight = 0.80

	# weight argument
	if(int(sys.argv[2]) != 0):
		calc_weight = (int(sys.argv[2]))/100


	#print("\n\nCALC WEIGHT: ",calc_weight,"\n\n")

	# For Iterative computation
	if(int(sys.argv[1]) > 0):

		for i in range(int(sys.argv[1])):

			contribs = links.join(ranks).flatMap(lambda x: computeContribs(x))

			ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank*calc_weight) + (1-calc_weight))

	# For computation until convergence
	else:
		while( not convergence):
			c = c + 1
			#print("\nIN WHILE LOOP\n")
			#print("ITERATION ",c,"\n")

			contribs = links.join(ranks).flatMap(lambda x: computeContribs(x))

			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*calc_weight + (1-calc_weight))

			new_ranks = ranks.sortByKey().collect()

			for i in range(total):

				diff = abs(old_ranks[i][1] - new_ranks[i][1])

				name = old_ranks[i][0]

				#print(old_ranks[i][0]," ",new_ranks[i][0]," ",old_ranks[i][1]," ",new_ranks[i][1]," ",diff)

				if(diff < 0.0001):
					convergence = True
				else:
					convergence = False
					#print("\nDid not converge - Iteration ",c,". Failed at - ",name," Difference - ",diff,"\n")
					break

			old_ranks = new_ranks


	new_ranks_output = ranks.collect()

	final_count = ranks.count()

	sorted_output = ranks.takeOrdered(final_count, key = lambda x: -x[1])

	# Print NEW RANKS
	#print("\n")
	for (bowler,rank) in sorted_output:
		print(bowler,round(rank,12))
	#print("\n")

	#print("\nNumber of Iterations: ",c)

	spark.stop()
