from __future__ import print_function
import re
import sys
from operator import add

import pyspark
import pyspark.sql.functions as sf
import pyspark.sql.types as sparktypes
from pyspark.sql import SparkSession
def parseNeighbors(urls):
    parts = re.split(r',',urls)
    return parts[0], parts[1]
def parseValues(names):
	parts=re.split(r',',names)
	avg=float(parts[2])/float(parts[3])
	return parts[1],parts[0]+","+str(avg)
def crank(urls):
    parts=re.split(r',',urls)
    return parts[1],float(float(parts[2])/float(parts[3]))

def computeInitialRank(key,value):
	sum=0.0
	for v in value:
		parts=re.split(r',',v)
		sum=sum+float(parts[1])
	if (sum<1):
		sum=1.0
	return key,sum

def computeContribs(bowlers,rank):
	num_bowls=len(bowlers)
	for bowler in bowlers:
		parts=re.split(r',',bowler)
		yield(parts[0],rank/num_bowls)


if __name__ == "__main__":
	if len(sys.argv) !=4:
		sys.exit(-1)

	spark = SparkSession\
		.builder\
 		.appName("Rank")\
 		.getOrCreate()
#	sqlc=pyspark.SQLContext(spark)
	lines=spark.read.text(sys.argv[1]).rdd.map(lambda r:r[0])
	links = lines.map(lambda urls: parseNeighbors(urls)).groupByKey().cache()
	count_num_iter=0
	reduced= lines.map(lambda urls: crank(urls)).groupByKey().reduceByKey(add).cache()
	num_of_iter=int(sys.argv[2])
	custom_weight=int(sys.argv[3])
	ranks = reduced.map(lambda x: (x[0], max(int(list(x[1])[0]),1)))
	oldrank=ranks


	if(num_of_iter!=0):
		for i in range(num_of_iter):
			contribs=links.join(ranks).flatMap(lambda ranx:computeContribs(ranx[1][0],ranx[1][1]))
			if(custom_weight==0):
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
			else:
				ranks= contribs.reduceByKey(add).mapValues(lambda rank: rank * custom_weight/100 + (1-custom_weight)/100)
	else:
		while(True):
			contribs=links.join(ranks).flatMap(lambda ranx:computeContribs(ranx[1][0],ranx[1][1]))
			if(custom_weight==0):
				ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
			else:
				ranks= contribs.reduceByKey(add).mapValues(lambda rank: rank * custom_weight/100 + (1-custom_weight/100))
			diff=ranks.join(oldrank).mapValues(lambda rank:abs(float(rank[0])-float(rank[1]))) # batsman [oldrank-newrank]
			flag=0
			for key,value in diff.collect():
				if(value>=0.0001):
					flag=1
			if(not flag):
				break;
			oldrank=ranks
			count_num_iter+=1
	
	xyz=ranks.sortBy(lambda a:(-a[1],a[0]))
	for (key,value) in xyz.collect():
		print(key,", ",value)
	#print(count_num_iter)
	spark.stop()
