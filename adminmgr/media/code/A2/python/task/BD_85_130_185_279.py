from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    print(num_urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    parts = urls.split(',')
    return parts[0],(float(parts[2])/float(parts[3]))

def parseNeighbor(urls):
    parts = urls.split(',')
    return parts[0],parts[1]
def avg_calc(x):
	z=x[0]/x[1]
	if (z>1):
		return z
	return 1
def itern():
	if(int(sys.argv[2])):
		return (int(sys.argv[2]))
	return (2000)
def perc():
	if(int(sys.argv[3])):
		return float(float(sys.argv[3])/100)
	return 0.2
	
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    
    a=itern()
    b=perc()
    #print(a,b)
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    links = lines.map(lambda urls: parseNeighbor(urls)).groupByKey().cache()
#    for (link, rank) in links.collect():
 
#        print("%s,%s." % (link, len(rank)))

    avg = lines.map(lambda urls: parseNeighbors(urls)).reduceByKey(lambda x, y: (x+y)).mapValues(lambda x: x if x>1 else 1)
    #balls = lines.map(lambda urls: parseNeighbors(urls,3)).reduceByKey(lambda x, y: (x+y))
    #avg= wickets.join(balls).mapValues(avg_calc)
#    for (link, rank) in avg.collect():
 #        print("%s,%s." % (link, rank))

    i=0
    flag=0
    while(i<a):
        contribs = links.join(avg).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
 		       
        navg = contribs.reduceByKey(add).mapValues(lambda rank: rank * (1-b) + b)
        i+=1
        #print(i)    
        if(a==2000):
            flag=1
            c= navg.join(avg).map(lambda r: (r[0],abs(r[1][1]-r[1][0])))  
            
            for x,y in c.collect():
               #print(x,y)
               if x is not None:
                if y>=0.0001:
                 flag=0
                 break
        if(flag==1):
               break
        avg=navg
					
        #avg2=avg1.join(avg).filter(lambda a: a[1][0]-a[1][1]>=0.0001). 
        #for (link, rank) in ranks.collect():
         #print("%s has rank: %s." % (link, rank))

       # contribs = links.join(avg)
    #    for (link, (rank,a)) in contribs.collect():
     #    print("%s has avg: %s %s." % (link, rank,a))
        #for (link, (rank,a)) in contribs.collect():
         #print("%s has avg: %s %s" % (link, len(rank),a))

       # avg = contribs.mapValues(lambda urls: computeContribs(urls[0],urls[1]))
        
    for (link, rank) in avg.sortBy(lambda x:x[0]).sortBy(lambda x:x[1],ascending=False).collect():
         print("%s,%.12f" % (link, rank))

    spark.stop()
