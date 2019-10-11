from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(plyrsfaced, rank):
    num_plyrsfaced = len(plyrsfaced)
    for plyr in plyrsfaced:
        yield (plyr, rank / num_plyrsfaced)

def compavg(bowler):
	parts=re.split(r',',bowler)
	return parts[1],float(int(parts[2])/int(parts[3]))

def checkconverge(rank,prevrank):
	count =0
	temp=rank.join(prevrank).collect()
	for i in range(len(temp)):
		if(abs(temp[i][1][0]-temp[i][1][1])<0.0001):
			#print("Diff is ",temp[i][1][0]-temp[i][1][1])
			count=count+1
	if(count==len(temp)):
		return True
	else:
		return False

def parseNeighbors(plyrs):
    parts = re.split(r',', plyrs)
    return parts[0], parts[1]


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    
    batbowlinks = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    
    inter= lines.map(lambda urls: compavg(urls))
    
    inter = inter.distinct().groupByKey()
    bowranks=inter.map(lambda att:(att[0],max(1,sum(att[1]))))

    if(int(sys.argv[3])==0):
    	prob1=0.8
    	prob2=0.2
    else:
    	prob1=int(sys.argv[3])/100
    	prob2=1-prob1
    if(int(sys.argv[2])==0):
    	i=0
    	prevrank=bowranks
    	while(not(checkconverge(bowranks,prevrank)) or i==0):
    		print("I am in:",i)
    		i=i+1
    		contribs = batbowlinks.join(bowranks).flatMap(lambda bbrank: computeContribs(bbrank[1][0], bbrank[1][1]))
    		prevrank=bowranks
    		bowranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * prob1 + prob2)

    	rankalph=bowranks.sortBy(lambda a:a[0])
    	ranksort=rankalph.sortBy(lambda a:-a[1])
    else:
    	i=0
    	prevrank=bowranks
    	for i in range(int(sys.argv[2])):
    		#print("I am in:",i)
    		i=i+1
    		contribs = batbowlinks.join(bowranks).flatMap(lambda bbrank: computeContribs(bbrank[1][0], bbrank[1][1]))
    		prevrank=bowranks
    		bowranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * prob1 + prob2)

    	rankalph=bowranks.sortBy(lambda a:a[0])
    	ranksort=rankalph.sortBy(lambda a:-a[1])
    	
    	
    for (link, rank) in ranksort.collect():
        #print("%s has rank: {0:.12f}." % (link, rank))
        print(link,"{0:.12f}".format(rank),sep=",")
	
    spark.stop()	
