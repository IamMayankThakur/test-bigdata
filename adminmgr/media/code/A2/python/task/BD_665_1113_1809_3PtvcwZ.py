from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession
def calsumofavg(line):
	return line[0],max(1,sum(list(line[1])))
def bowling(line):
	parts = re.split(',', line)
	return parts[1], float(int(parts[2])/int(parts[3]))	
def subtracts(now,before):
	if(abs(now-before)<0.0001 or abs(before-now)<0.0001):
		return True
	return False

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    parts = re.split(',', urls)
    return parts[0], parts[1]
def allbatsman(line):
	parts = re.split(',', line)
	return parts[0]
def allbowler(line):
	parts = re.split(',', line)
	return parts[1]
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)
    if int(sys.argv[2])<0 or int(sys.argv[3])<0 :
    	sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    
    initialrank=lines.map(lambda urls: bowling(urls)).groupByKey().cache()
    ranks=initialrank.map(lambda player_avg:calsumofavg(player_avg))
    allbatsman=lines.map(lambda urls: allbatsman(urls)).distinct()
    allbowler=lines.map(lambda urls: allbowler(urls)).distinct()
    lazybowlers=allbatsman.subtract(allbowler)
    rankslazy = lazybowlers.map(lambda lazy: (lazy[0], 1))
    #ranks=ranks.union(rankslazy)
     
    #k=links.collect()
    #for i in k:
    #	print(i[0],list(i[1]))

    #lazybatsmen=allbowler.subtract(allbatsman) 
    #print(len(allbatsman.collect()))
    #print(len(allbowler.collect()))
    #print(lazybatsmen.collect()) 
    #print(len(ranks.collect()))
    #k=initialrank.collect()
    #for i in k:
    #	print(i[0],list(i[1]))

    #print(links.glom().collect())
    #i=links.glom().collect()
    #print(i[0])
    #for j in i[0]:
    #	print(j[0],list(j[1]))
    #initialrank=lines.map(lambda )
    
    #ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    
    #print(ranks.glom().collect())
    #print(links.join(ranks).glom().collect())
    
    iters=0
    cond=True
    x=int(sys.argv[3])
    if x==0:
     damp=0.8
    else:
     damp=float(x/100)
    if int(sys.argv[2])==0:
     while(cond  ):
         contribs = links.join(ranks).flatMap(
             lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
         #print(contribs.glom().collect())
         #print( contribs.reduceByKey(add).glom().collect())
         prev=ranks

         ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * damp + 1-damp)
	 #aranks=ranks.collect()
         #aprev=prev.collect()
         #print(ranks.join(prev).collect())
         tocontinue=ranks.join(prev).map(lambda url_rankn_rankb:subtracts(url_rankn_rankb[1][0],url_rankn_rankb[1][1]))	
         #print("rank",ranks.collect())
         #print(tocontinue)
         torf=tocontinue.reduce(lambda x,y:x and y)
         if(torf==True):
               	 cond=False
         iters=iters+1
     #print(iters)
    else:
     for iteration in range(int(sys.argv[2])):
         contribs = links.join(ranks).flatMap(
             lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
         #print(contribs.glom().collect())
         #print( contribs.reduceByKey(add).glom().collect())
         #prev=ranks
         ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * damp + 1-damp)

    
    fakesorted = ranks.sortBy(lambda a: a[0])
    bsorted = fakesorted.sortBy(lambda a: -a[1])
    #raw_data = raw_data.withColumn("LATITUDE_ROUND", format_number(raw_data.LATITUDE, 3))
    #bSorted.collect()
    for (link, rank) in bsorted.collect():
        print("%s,%s" % (link,"{0:.12f}".format(rank)))

    spark.stop()
