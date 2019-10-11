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



def parseNeighbors(batsman):
        parts = re.split(r'\,', batsman)
        return parts[0],(float(parts[2]) / float(parts[3]))
def parseNeighbors1(batsman):
        parts = re.split(r'\,', batsman)
        return parts[0],(parts[1])
        
def sum1(z):
   u=0
   
   for i in range(len(z)):
      u=u+float(z[i])
   if(u>1):
       return float(u)
   else:
       return 1.0
       


if __name__ == "__main__":
        if len(sys.argv) != 3:
	        print("Usage: pagerank <number_iterations> <rank_weights>", file=sys.stderr)
                sys.exit(-1)

	# Initialize the spark context.
        spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

        lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

        links = lines.map(lambda batsman: parseNeighbors(batsman)).distinct().groupByKey().cache()
        
        links1=lines.map(lambda batsman: parseNeighbors1(batsman)).distinct().groupByKey().cache()
        
        
	link=links.map(lambda x:(x[0],float(sum1(list(x[1])))))
        #print(links.collect())
        ranks = link.map(lambda url_neighbors: (url_neighbors[0],url_neighbors[1]))
        #print(ranks.collect())
        iterations=0
        if(int(sys.argv[2])==0):
        
      
        
        
            flag=0
            iterations=0
            while flag == 0 and iterations < 2000:
                iterations = iterations + 1
                prev_ranks = ranks
                contribs = links1.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            
            

            

                ranks = contribs.reduceByKey(add).mapValues(lambda rank:float(rank * 0.80 + 0.20))
                r1=prev_ranks
                r2=ranks
                flag=1
                comp = r1.leftOuterJoin(r2).map(lambda r: diff(r))
                for x in comp.collect():
                    if x is not None:
                        if float(x[1]) >= 0.0001:
                            flag=0
        elif int(sys.argv[2]) >0:
            iterations=0
            for iteration in range(int(sys.argv[2])):
                iterations = iterations + 1
                contribs = links1.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
                y=(100-float(sys.argv[2]))/100
                x=1-y
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * y + x)
        rankss=ranks.sortBy(lambda a:(-a[1],a[0]))
        #print("%d iterations \n" %(int(sys.argv[2])))
        for (link, rank) in rankss.collect():
            #print(link + ", " + "{:.12f}".format(rank))
             print("%s,%0.12f" % (link, rank))
       
        
        
        spark.stop()
