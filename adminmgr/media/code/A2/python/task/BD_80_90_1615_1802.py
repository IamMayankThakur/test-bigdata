from __future__ import print_function

import re
import sys
import operator
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
	num_urls = len(urls)
	for url in urls:
		yield (url, rank / num_urls)
if __name__ == "__main__":
	if len(sys.argv) > 4:
		print("Usage: pagerank <file> <iterations>", file=sys.stderr)
		sys.exit(-1)
	spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda x:x[0])#read line by line.


	#To get the initial ranks:
	lines=lines.map(lambda splitter:splitter.split(","))
	bowl_avg=lines.map(lambda bowler:(bowler[1],(int(bowler[2])/int(bowler[3]))))
	total_avg = bowl_avg.reduceByKey(lambda x, y: x + y)

	#To see how many players a player is linked to:
	links=lines.map(lambda x: (x[0],x[1])).distinct().groupByKey().cache()
	temp_join=links.join(total_avg)
	ranks=temp_join.map(lambda x:(x[0],max(1.0,x[1][1])))
	
	#Page Rank Computation
	if(int(sys.argv[2])!=0):
		attr=(int(sys.argv[3]))/100
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * attr + (1-attr))
	else:
		temp_ranks=ranks
		contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
		diff=temp_ranks.join(ranks).filter(lambda x:(x[1][0] is not None) and (x[1][1] is not None) and abs(x[1][0]-x[1][1])>0.0001)
		while(not diff.isEmpty()):
			temp_ranks=ranks
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
			diff=temp_ranks.join(ranks).filter(lambda x:(x[1][0] is not None) and (x[1][1] is not None) and abs(x[1][0]-x[1][1])>0.0001)
	ranks=ranks.sortBy(lambda x:x[0])#Sort in lexographic player order.
	ranks=ranks.sortBy(lambda x:-x[1])#Sort in descending based on keys.
		
	#Print Stage
	for (link, rank) in ranks.collect():
		print(link, "{:.12f}".format(rank),sep = ",")
	spark.stop()
