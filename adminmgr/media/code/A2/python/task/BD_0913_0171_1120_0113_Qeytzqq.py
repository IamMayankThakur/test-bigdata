from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession

def diff(r):
    if r[1][0] is not None:
	    if r[1][1] is not None:
		    d = abs(r[1][0]-r[1][1])
		    return r[0],d
def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls):
    parts = re.split(r'\,+', urls)
    return parts[0], parts[1]

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])


    l = lines.map(lambda y: (y.split(",")[0] + "," + y.split(",")[1]))
    #print(l)
    ranks1 = lines.map(lambda y: (y.split(",")[1],int(y.split(",")[2])/int(y.split(",")[3])))
    r = ranks1.reduceByKey(lambda x, y: x + y)
    ranks = r.map(lambda a : (a[0], max(a[1],1.0)))
    

    links = l.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
  
    weight = int(sys.argv[3]) * 0.01	#Conversion of percentage to value
    

    if (int(sys.argv[2])) == 0:
        flag = 0
	iterations=0
	while flag == 0 and iterations < 2000:
	    iterations= iterations+1
	    pre_ranks=ranks
	    contribs = links.join(ranks).flatMap(
		    lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
	    if (int(sys.argv[3])) == 0:
		    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.2)
	    else:
		    ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank * weight) + (1-weight))
	    r1=pre_ranks
	    r2=ranks
	    flag=1
	    c= r1.leftOuterJoin(r2).map(lambda r:diff(r))
	    for x in c.collect():
		if x is not None and x[1]>=0.0001:
                    flag=0
    else:
        for iteration in range(int(sys.argv[2])):
	    contribs = links.join(ranks).flatMap(
		    lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

	    if (int(sys.argv[3])) == 0:
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.2)
	    else:
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank * weight) + (1-weight))
    ranks_final = ranks.sortBy(lambda x : (-x[1],x[0]))
		
    for (link, rank) in ranks_final.collect():
        print(link, "{:.12f}".format(rank),sep = ",")
    print()
    spark.stop()
