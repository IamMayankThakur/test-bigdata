#!/usr/bin/python
from __future__ import print_function

import re
import sys
from operator import add



import pyspark
#import 2 more
import pyspark.sql.functions as sf
import pyspark.sql.types as sparktypes
from pyspark.sql.functions import desc

from pyspark.sql import SparkSession


def Bowling_Average(urls):
    parts = re.split(r',', urls)
    #calculate the bowling average
    result = float(parts[2])/float(parts[3])
    yield (parts[1],result)

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url[0], rank / num_urls)

def parseNeighbors(urls):
    parts = re.split(r',', urls)
    result = float(parts[2])/float(parts[3])
    #return bowler, batsman and result which is the bowling average
    return parts[0],(parts[1],result)




if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: pagerank <file> <iterations>", file=sys.stderr)
		sys.exit(-1)

	# Spark Context initialization to use PySpark
	spark = SparkSession\
	.builder\
	.appName("PythonPageRanktoRankBowlers")\
	.getOrCreate()

	#itr = int(sys.argv[2])
	#wht = int(sys.argv[3])
	
	flag = 1	
	increment_variable = 1

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	#function used to get the bowlers names along with the batsman name and the bowling average
	links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
	#to calculate the bowling average of the bowlers
	prev_ranks = lines.flatMap(lambda bowlers: Bowling_Average(bowlers)).reduceByKey(add).map(lambda url_neighbors: (url_neighbors[0],max(url_neighbors[1],1.0)))
	
	weightage = int(sys.argv[3])	
	if((int(sys.argv[2])) == 0):
		if((int(sys.argv[3])) == 0):	
			while(flag):
				contribs = links.join(prev_ranks).flatMap(
			   	 lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
				#to calculate the present rank
				current_rank = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
				#to sort the given current nd previous ranks
				#to sort the previous ranks
				sorted_prev_ranks = sorted(prev_ranks.collect(), key = lambda variable: variable[0])
				#to sort the present ranks
				sorted_pres_ranks = sorted(current_rank.collect(), key = lambda variable: variable[0])
				#a flag variable to keep track if the condition becomes 0
				flag = 0
				#to calculate and see if the value is less then the convergence factor
				#need to take the absolute value of the difference
				num = prev_ranks.count()
				for j in range(0,num):
					if(abs(sorted_prev_ranks[j][1]-sorted_pres_ranks[j][1]) > 0.0001):
						#initialize flag to 1
						flag  = 1
						break
				prev_ranks = current_rank
				#operation: need to increment the counter variable
			
				#increment is a counter variable
				increment_variable+=1	
				
		elif ((int(sys.argv[3])) != 0):
			while(flag):
				contribs = links.join(prev_ranks).flatMap(
			   	 lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
				#to calculate the present rank
				current_rank = contribs.reduceByKey(add).mapValues(lambda rank: rank *(weightage/100) + ((100-weightage)/100))
				#to sort the given current nd previous ranks
				#to sort the previous ranks
				sorted_prev_ranks = sorted(prev_ranks.collect(), key = lambda x: x[0])
				#to sort the present ranks
				sorted_pres_ranks = sorted(current_rank.collect(), key = lambda x: x[0])
				#a flag variable to keep track if the condition becomes 0
				flag = 0
				#to calculate and see if the value is less then the convergence factor
				#need to take the absolute value of the difference
				num = prev_ranks.count()
				for i in range(0,num):
					if(abs(sorted_prev_ranks[i][1]-sorted_pres_ranks[i][1]) > 0.0001):
						#initialize flag to 1
						flag  = 1
						break
				prev_ranks = current_rank
				#operation: need to increment the counter variable
				
				#increment is a counter variable
				increment_variable+=1	
		
	elif ((int(sys.argv[2])) != 0):
		if((int(sys.argv[3])) == 0):
			while(flag and (increment_variable!=(int(sys.argv[2])))):
				contribs = links.join(prev_ranks).flatMap(
			   	 lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
				#to calculate the present rank
				current_rank = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
				#to sort the given current nd previous ranks
				#to sort the previous ranks
				sorted_prev_ranks = sorted(prev_ranks.collect(), key = lambda x: x[0])
				#to sort the present ranks
				sorted_pres_ranks = sorted(current_rank.collect(), key = lambda x: x[0])
				#a flag variable to keep track if the condition becomes 0
				flag = 0
				num = prev_ranks.count()
				for i in range(0,num):
					if(abs(sorted_prev_ranks[i][1]-sorted_pres_ranks[i][1]) > 0.0001):
						#initialize flag to 1
						flag  = 1
						break
				prev_ranks = current_rank
				#operation: need to increment the counter variable
				#print(increment_variable)
				#increment is a counter variable
				increment_variable+=1	
		if((int(sys.argv[3])) != 0):
			while(flag and (increment_variable!=(int(sys.argv[2])))):
				contribs = links.join(prev_ranks).flatMap(
			   	 lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
				#to calculate the present rank
				current_rank = contribs.reduceByKey(add).mapValues(lambda rank: rank *(weightage/100) + ((100-weightage)/100))
				#to sort the given current nd previous ranks
				#to sort the previous ranks
				sorted_prev_ranks = sorted(prev_ranks.collect(), key = lambda x: x[0])
				#to sort the present ranks
				sorted_pres_ranks = sorted(current_rank.collect(), key = lambda x: x[0])
				num = prev_ranks.count()
				#a flag variable to keep track if the condition becomes 0
				flag = 0
				for i in range(0,num):
					if(abs(sorted_prev_ranks[i][1]-sorted_pres_ranks[i][1]) > 0.0001):
						#initialize flag to 1
						flag  = 1
						break
				prev_ranks = current_rank
				#operation: need to increment the counter variable
				
				#increment is a counter variable
				increment_variable+=1	
				
		#print(increment_variable)
			
	#to sort: final operation
	for (bowler, final) in sorted(prev_ranks.collect(), key = lambda y: y[1], reverse = 1):
		print("%s,%s" % (bowler, final))

	spark.stop()
