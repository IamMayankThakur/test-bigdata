from __future__ import print_function

import re
import sys
from operator import add
from pyspark.sql import SparkSession
def computeContribs(urls, rank):
	num_urls = len(urls)
	for url in urls:
		yield (url, rank / num_urls)
def initialiseRanks(urls):
	parts = re.split(r',', urls)
	return parts[0],int(parts[2])/int(parts[3])
def formLinks(urls):
	parts = re.split(r',',urls)
	return parts[0],parts[1]

if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: pagerank <file> <iterations>", file=sys.stderr)
		sys.exit(-1)

    # Initialize the spark context.
	spark = SparkSession\
		.builder\
		.appName("PythonPageRank")\
		.getOrCreate()

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	initial_ranks = lines.map(lambda urls: initialiseRanks(urls)).distinct().groupByKey().mapValues(sum).cache()
	ranks=initial_ranks.map(lambda x:(x[0],max(x[1],1)))
	links = lines.map(lambda urls: formLinks(urls)).distinct().groupByKey().cache()
	count = 0
	weight = int(sys.argv[3])
	if(weight == 0):
		weight = 0.80
	else:
		weight = int(sys.argv[3])/100
	if(int(sys.argv[2]) == 0):
		temp2 = 1000000
		while(True):
			contribution = links.join(ranks)
			each_pair = contribution.flatMap(lambda bowler_rank: computeContribs(bowler_rank[1][0], bowler_rank[1][1]))
			ranks = each_pair.reduceByKey(add).mapValues(lambda rank: rank * weight +(1 - weight))
			result=sorted(ranks.collect(),key=lambda y:(-y[1],y[0]))	
			#count = count + 1
			temp1 = result[0][1]
			#print(temp1)
			#print(temp2)
			#print(temp2-temp1)
			if((temp2 - temp1) < 0.00001):
				break
			temp2=temp1
	else:
		for iteration in range(int(sys.argv[1])):
			contribution = links.join(ranks)
			each_pair = contribution.flatMap(lambda bowler_rank: computeContribs(bowler_rank[1][0], bowler_rank[1][1]))
			ranks = each_pair.reduceByKey(add).mapValues(lambda rank: rank * weight +(1 - weight))
	
	result=sorted(ranks.collect(),key=lambda y:(-y[1],y[0]))
	for (link,rank) in result:
		print("%s,%.12f" %(link, rank))
	#print("count")
	#print(count)
	spark.stop()	

