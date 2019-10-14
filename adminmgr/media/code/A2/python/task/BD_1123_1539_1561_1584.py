from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
	num_urls = len(urls)
	for url in urls:
		yield (url, rank / num_urls)

#Splitting the input to two halves(desired). In the hands on ex, first line is 1 2 . So parts[0]=1 and parts[1]=2
def parseNeighbors(urls):
	parts = urls.split(',')
	return parts[0], parts[1]

def bowl_average(urls):
	parts = urls.split(',')
	return parts[1], float((int(parts[2]))/(int(parts[3])))

def is_converged(old_rank,new_rank):
	old = old_rank.collect()
	counter = old_rank.join(new_rank).map(lambda instance : abs(instance[1][0]-instance[1][1]).filter(lambda x:x<0.001).count())
	if counter == len(old):
		return 1
	else:
		return 0
    

#Start of execution of PySpark program.
if __name__ == "__main__":
	#Command line should contain 3 arguments or exit.
	if len(sys.argv) != 4:
		print("Usage: pagerank <file> <iterations>", file=sys.stderr)
		sys.exit(-1)

    # Initialize the spark context.
	spark = SparkSession\
		.builder\
		.appName("PythonPageRank")\
		.getOrCreate()
		
		#Get each line of the input dataset
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
		
		#Create lines for each line (Get all the values for a particular key.i.e for ex get all the bowlers who have bowled for a particular batsman.
	links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

	averages = lines.map(lambda urls:bowl_average(urls)).distinct().groupByKey().cache()

		#Initialize the rank for each key to 1.0(In the assignment you have to assign the sum of averages for a particular batsman)
	ranks = averages.map(lambda url_neighbors: (url_neighbors[0], max(sum(url_neighbors[1]),1.0)))
		
		#sys.argv[2] will give the number of iterations.(The number of times the vector has to be multiplied with the matrix. In the assignment, sys.argv[2] will be 0 for convergence or >0 if number of iterations is specified.)  #Refer the slides for flatMap function.
	iterations = int(sys.argv[2])
	weight_r=int(sys.argv[3]) #Weight read from the command line.
	weight=0
	if(weight_r>0):
		weight=weight_r/100
	elif(weight_r==0):
		weight=80/100

	if iterations > 0:
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			
					    #Adding weights to the rank.(In the assignment the weight will be specified in the command line and that value must be used.
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1-weight))
		
		name_sorted = ranks.sortBy(lambda x: x[0],ascending= True)
		rank_sorted = name_sorted.sortBy(lambda x:x[1],ascending = False)
	
	elif iterations==0:
		contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1-weight))
		#old_ranks = ranks
		#contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
		#ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1-weight))
		
		while(True):
			old_ranks = ranks
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1-weight))
			if(is_converged(old_ranks,ranks)):
				break
		name_sorted = ranks.sortBy(lambda x: x[0],ascending= True)
		rank_sorted = name_sorted.sortBy(lambda x:x[1],ascending = False)
		#rank_sorted = is_converged(old_ranks,ranks)
			
	for (link, rank) in rank_sorted.collect():
		print("%s,%s." % (link, rank))

	spark.stop()