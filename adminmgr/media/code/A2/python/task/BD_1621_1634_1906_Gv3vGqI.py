from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(batsmen, rank):
	num_batsmen = len(batsmen)
        for batsman in batsmen:
            yield (batsman, float(rank) / float(num_batsmen))
def diff(r):
    if r[1][0] is not None:
        if r[1][1] is not None:
            d = abs(r[1][0]-r[1][1])
            return r[0],d
def parseNeighbors(batsman,i):
        if(i==0):
            parts = re.split(r'\,', batsman)
            return parts[0],(float(parts[2]) / float(parts[3]))
        elif(i==1):
            parts = re.split(r'\,', batsman)
            return parts[0],(parts[1])
if __name__ == "__main__":
        if len(sys.argv) != 4:
	        print("Usage: pagerank <number_iterations> <rank_weights>", file=sys.stderr)
                sys.exit(-1)

	# Initialize the spark context.
        spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

        lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

        links = lines.map(lambda batsman: parseNeighbors(batsman,0)).distinct().groupByKey()
        
        links1=lines.map(lambda batsman: parseNeighbors(batsman,1)).distinct().groupByKey()
        
        link=links.map(lambda x:(x[0],sum(x[1]) if sum(x[1])>1.0 else 1.0))
        
        ranks = link.map(lambda url_neighbors: (url_neighbors[0],url_neighbors[1]))
        
       
        if float(sys.argv[3]) > 0:
		slope = float(sys.argv[3]) / 100
	else:
		slope = float(80) / 100

	intercept = 1 - slope
        if int(sys.argv[2])==0:
            iteration=0
            flag=0
            
            while flag == 0 and iteration<2000:
                prev_ranks = ranks
                iteration=iteration+1
                contribs = links1.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * slope + intercept)
                r1=prev_ranks
                r2=ranks
                flag=1
                comp = r1.leftOuterJoin(r2).map(lambda r: diff(r))
                for x in comp.collect():
                    if x is not None:
                        if x[1] >= 0.0001:
                            flag=0
        elif int(sys.argv[2])>0:
            
            for iteration in range(int(sys.argv[2])):
                
                contribs = links1.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
                ranks = contribs.reduceByKey(add).mapValues(lambda rank:  rank * slope + intercept)
        rankss=ranks.sortBy(lambda a:(-a[1],a[0]))
        for (link, rank) in rankss.collect():
            print("%s,%0.12f" % (link, rank))
        spark.stop()
