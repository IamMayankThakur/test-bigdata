from __future__ import print_function

import re
import sys
from operator import add
	
from pyspark.sql import SparkSession

def computeContribs(urls, rank):
	u = list(urls)
	if(u[0] != 'no'):
		num_urls = len(urls)
		for url in urls:
			yield (url, rank / num_urls)













    
def parseNeighbors(urls):
    parts = re.split(r',+', urls)
    return parts[0], parts[1] 









def computeRanks(x) :
	parts = re.split(r',+',x)
	return parts[1],int(parts[2])/int(parts[3])



if __name__ == "__main__":
	spark = SparkSession\
        .builder\
        .appName("PythonPageRank1")\
        .getOrCreate()
	df = spark.read.csv(sys.argv[1])
	lines = df.rdd.map(lambda x: x[0]+','+x[1]+','+x[2]+','+x[3])
	links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
	ranks1 = lines.map(lambda x : computeRanks(x)).reduceByKey(add)
	ranks = ranks1.map(lambda x : (x[0],max(x[1],1)))
	n = ranks.count()
	old_ranks = ranks.map(lambda x : (x[0],0))
	while(1):
		contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
		joined = ranks.join(old_ranks).map(lambda x : x[1][0] - x[1][1])		
		filtered = joined.filter(lambda x : x <= 0.0001)	
		cnt = filtered.count()
		if(cnt == n):		
			break	
		old_ranks = ranks	  
	ranks = ranks.sortBy(lambda x : (-x[1],x[0]))
	for x in ranks.collect() :		
		print(x[0],'{0:.12f}'.format(x[1]))	
	spark.stop()

