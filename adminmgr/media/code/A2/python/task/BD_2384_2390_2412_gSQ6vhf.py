
	
from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession
import sys


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)
	
if __name__ == "__main__":
	percent = int(sys.argv[3])/100
	if (percent == 0):
		percent = 0.8
	else:
		percent = int(sys.argv[3])/100
	spark = SparkSession.builder.appName("pageRank").getOrCreate()
	
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	
		# every bowler with the set of players who faced him ***links go from batsman to bowler***
	links = lines.map(lambda x: x.split(',')).map(lambda x: (x[0],x[1])).groupByKey().cache()	#x[0] -> batsman & x[1] -> bowler
	
		# using max(sum(averages),1) for each bowler's inital rank
		#initial_ranks = lines.map(lambda x: x.split(',')).map(lambda x: (x[1],1) if ((int(x[2])/int(x[3])) < 1) else (x[1],int(x[2])/int(x[3])) ).reduceByKey(add)
	initial_ranks = lines.map(lambda x: x.split(',')).map(lambda x: (x[0],int(x[2])/int(x[3]))).reduceByKey(add)
	
	ranks = initial_ranks.map(lambda x: (x[0],1) if (x[1]<1) else (x[0],x[1]))
	
	
	old_ranks = ranks.collect()	
	
	convergence = False
	c = 0
	
		# For Iterative computation
	if(int(sys.argv[2]) > 0):
	
		for i in range(int(sys.argv[2])):
	
				#contribs = links.join(ranks).flatMap(lambda x: computeContribs(x))
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

			ranks = contribs.reduceByKey(add).mapValues(lambda rank:( rank* percent +(1- percent)) )
	
		# For computation until convergence
	else:
		while( not convergence):
			c = c + 1
				#print("\nIN WHILE LOOP\n")
				#print("ITERATION ",c,"\n")
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
				#contribs = links.join(ranks).flatMap(lambda x: computeContribs(x))
	
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank* percent + (1-percent)) )
			new_ranks = ranks.collect()	
			
			for i in range(10):
	
				diff = abs(old_ranks[i][1] - new_ranks[i][1])
	
					#name = old_ranks[i][0]
	
					#print(old_ranks[i][0]," ",new_ranks[i][0]," ",old_ranks[i][1]," ",new_ranks[i][1]," ",diff)
	
				if(diff < 0.00001):
					convergence = True
					#else:
					#	convergence = False
						#print("\nDid not converge - Iteration ",c,". Failed at - ",name," Difference - ",diff,"\n")
						#break
	
			old_ranks = new_ranks
		
		#new_ranks_output = ranks.collect()
	
		#final_count = ranks.count()
	final = sorted(ranks.collect(),key = lambda x: (-x[1],x[0]))
	for (link, rank) in final:
		print("%s,%.12f" % (link, rank))

	spark.stop()	

