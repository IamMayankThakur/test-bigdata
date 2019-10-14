from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession
import sys
import findspark
findspark.init()
def computeContribs(merged_tuple);
	batsman_list = merged_tuple[1][0]	
	current_rank = merged_tuple[1][1]	
	batsman_number = len(batsman_list)
	for n in batsman_list:
		yield(n, current_rank/batsman_number)
if __name__ == "__main__":
	spark = SparkSession.builder.appName("batsmanRank").getOrCreate()
	lines = spark.read.text("hdfs://localhost:9000/input/BatsmanRankTestData.txt").rdd.map(lambda r: r[0])
	links = lines.map(lambda x: x.split(',')).map(lambda x: (x[0],x[1])).groupByKey().cache()
	initial_ranks = lines.map(lambda x: x.split(',')).map(lambda x: (x[1],int(x[2])/int(x[3]))).reduceByKey(add)
	ranks = initial_ranks.map(lambda x: (x[0],1) if (x[1]<1) else (x[0],x[1]))
	old_ranks = ranks.sortByKey().collect()
	total = initial_ranks.count()
	print("\n")
	for (bowler,avg) in old_ranks:
		print(bowler,avg)
	print("\n")
	print("\n Ranks Count:",total,"\n\n")
	links_output = links.collect()
	print("\n")
	for (bowler,l) in links_output:
		print(bowler,list(l),"\n")
	print("\n")
	print("\n Link Count:",links.count(),"\n\n")
	convergence = False
	c = 0
	if(int(sys.argv[1]) > 0):
		for i in range(int(sys.argv[1])):
			contribs = links.join(ranks).flatMap(lambda x: computeContribs(x))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*0.80 + 0.20)
	else:
		while( not convergence):
			c = c + 1
			print("\nIN WHILE LOOP\n")
			print("ITERATION ",c,"\n")
			contribs = links.join(ranks).flatMap(lambda x: computeContribs(x))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*0.80 + 0.20)
			new_ranks = ranks.sortByKey().collect()
			for i in range(total):
				diff = abs(old_ranks[i][1] - new_ranks[i][1])
				name = old_ranks[i][0]
				print(old_ranks[i][0]," ",new_ranks[i][0]," ",old_ranks[i][1]," ",new_ranks[i][1]," ",diff)
				if(diff < 0.0001):
					convergence = True
				else:
					convergence = False
					print("\nDid not converge - Iteration ",c,". Failed at - ",name," Difference - ",diff,"\n")
					break
			old_ranks = new_ranks
	new_ranks_output = ranks.collect()
	final_count = ranks.count()

