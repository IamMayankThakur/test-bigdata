from __future__ import print_function
import re
import sys
from operator import add,attrgetter
from pyspark.sql import SparkSession




def sortfunc():
	return (lambda x:(-x[1],x[0]))


def parNeighfunc1(z):
	chunks = re.split(r',', z)
	return chunks[0],chunks[1]

def parNeighfunc2(y):
	chunks = re.split(r',', y)
	return chunks[0],float(chunks[2])/float(chunks[3])

def ContributionComputation(x, y):
	z = len(x)
	for i in x:
		yield (i, float(y) / float(z))

def max_rank(x1,x2):
	if(int(x2)<1):
		return x1,1
	return x1,x2




spark = SparkSession\
	.builder\
	.appName("PythonPageRank")\
	.getOrCreate()
	
rows1 = spark.read.text(sys.argv[1])
rows = rows1.rdd.map(lambda r: r[0])

	
links_temp1 = rows.map(lambda urls: parNeighfunc1(urls))
links_temp2 = links_temp1.distinct()
links_temp3 = links_temp2.groupByKey()
links = links_temp3.cache()


links1_temp1 = rows.map(lambda url:parNeighfunc2(url))
links1_temp2 = links1_temp1.distinct()
links1_temp3 = links1_temp2.groupByKey()
links1_temp4 = links1_temp3.mapValues(sum)
links1      = links1_temp4.cache()

	
ranks= links1.map(lambda key:max_rank(key[0],key[1])).cache()


if(int(sys.argv[2]) == 0):
	i = False
	
	while(not(i)):
		c = 0
		c = c + 1
		contributions1 = links.join(ranks)
		contributions  = contributions1.flatMap(lambda batsman_bowler_rank: ContributionComputation(batsman_bowler_rank[1][0], batsman_bowler_rank[1][1]))
	
		temprank1 = contributions.reduceByKey(add)
		temprank  = temprank1.mapValues(lambda rank: rank * 0.80 + 0.20)

		out1 = sorted(temprank.collect(),key = sortfunc())
		
		i = 1
		j = 10000
		
		precision = float(i)/float(j)
				 
		out2 = sorted(ranks.collect(),key = sortfunc())
		valcheck = abs(out1[0][1] - out2[0][1])
		ranks = temprank
		
		if(abs(out1[0][1] - out2[0][1]) > pow(10,-4)):
			i =  False
		
		else:
			i = True

	for (links, ranks) in out2:
		print("%s,%0.12f" % (links,ranks))
		
		

  	 	

else:
	givenweight=float(sys.argv[3])
	
	for iteration in range(int(sys.argv[2])):
		c = 0
		c = c + 1
		contributions1 = links.join(ranks)
		contributions  = contributions1.flatMap(lambda batsman_bowler_rank: ContributionComputation(batsman_bowler_rank[1][0], batsman_bowler_rank[1][1]))

		ranks1 = contributions.reduceByKey(add)
		ranks = ranks1.mapValues(lambda rank: float(rank)*givenweight + (1 - givenweight))
		
	
	ranks = sorted(ranks.collect(),key= sortfunc())
	for (link, rank) in ranks:
		print("%s,%0.12f" % (link,rank))
  	

spark.stop()
