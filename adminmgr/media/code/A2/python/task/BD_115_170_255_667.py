from pyspark.sql import SparkSession
from operator import add
import re
import sys

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url[0], rank / num_urls)

def sumof(urls):
    sum = 0
    for url in urls:
        sum = sum+(url[1]/url[2])
    return max(1.0,sum)

def parseNeighbors(parts):
    return parts[0], (parts[1],int(parts[2]),int(parts[3]))

def convergence(ranks,old_ranks):
    i=0;
    n = len(ranks.collect())
    o=ranks.sortBy(lambda a:a[0]).collect()
    ne=old_ranks.sortBy(lambda a: a[0]).collect()
    while(i<n and abs(o[i][1]-ne[i][1])<0.0001):
        i=i+1
    if i > (n-1):
            return 0
    return 1

# Initialize the spark context.


if __name__ == "__main__":
	
	iteration = int(sys.argv[2]) 	
	weights = int(sys.argv[3])
	if weights == 0:
		weights = 80

	spark = SparkSession\
		.builder\
		.appName("PythonPageRank")\
		.getOrCreate()

	

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0].split(','))
	links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
	ranks = links.map(lambda url_neighbors: (url_neighbors[0], sumof(url_neighbors[1])))

	if(iteration ==0):
		old_ranks = ranks
		contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (weights/100) + 1-(weights/100))
	
		while(convergence(ranks,old_ranks)):
    			old_ranks = ranks
    			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (weights/100) + 1-(weights/100))
	else:
		for i in range(iteration):
			old_ranks = ranks
			contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (weights/100) + 1-(weights/100))
		
	for (link, rank) in ranks.sortBy(lambda x: x[1],False).collect():
    		print("%s, %s" % (link, round(rank,12)))

	spark.stop()
