from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession

spark = SparkSession.builder\
		.appName("Batsman prowess")\
		.getOrCreate()
#/home/hadoop/Desktop/Assignment-2/BatsmanRankTestData.txt

def checkConvergence(A,B):
	c=A.join(B).map(lambda x: abs(x[1][0] - x[1][1]))
	for difference in c.collect():
		if(difference > 0.0001):
			return False
	return True

def computeContribs(neighbors_rank):
	neighbors = neighbors_rank[1][0]
	rank = float(neighbors_rank[1][1])
	num_neighbors =len(neighbors)
	for n in neighbors:
		yield (n,rank /num_neighbors)

# def func(x):
# 	neighbors= x[1][0]
# 	rank=float(x[1][1])
# 	num_neighbors=len(x)
# 	for n in neighbors:
# 		yield(n,rank/num_neighbors)

input_lines=spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
#input_lines=spark.read.text("/home/hadoop/Desktop/Assignment-2/BatsmanRankTestData.txt").rdd.map(lambda r: r[0])
# lines=input_lines.map(lambda x:x.split(",")).cache()
lines=input_lines.map(lambda x:x.split(","))

ranks=lines.map(lambda x:(x[0],float(x[2])/float(x[3]))).reduceByKey(add)
# links=lines.map(lambda x:(x[1],x[0])).groupByKey().map(lambda x:(x[0],x[1]))  resultiterable problem
links=lines.map(lambda x:(x[0],x[1])).groupByKey().map(lambda x:(x[0],list(x[1])))
# contribs=links.join(ranks).flatMap(lambda x: func(x))
# print(contribs.collect())
try:
	weight=float(sys.argv[3])
except:
	weight=0

if(weight==0):
	weight=80

if(int(sys.argv[2])==0):
	while 1:
		old_rank=ranks
		contribs=links.join(ranks).flatMap(lambda x: computeContribs(x))
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: float(rank)*weight/100.0 + (100-weight)/100.0)
		if(checkConvergence(old_rank,ranks)):
			break 
else:
	for iteration in range(int(sys.argv[2])):
		contribs=links.join(ranks).flatMap(lambda x: computeContribs(x))
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: float(rank)*weight/100.0 + (100-weight)/100.0 )
	# print(ranks.collect())


output=sorted(ranks.collect(),key=lambda x: x[1],reverse= True)
for (bowler,rank) in output:
	print("%s,%.12f" % (bowler,rank))
spark.stop()

"""
from operator import add
def checkConvergence(A,B):
	c=A.join(B).map(lambda x: abs(x[1][0] - x[1][1]))
	for difference in c.collect():
		if(difference > 0.0001):
			return False
	return True

def computeContribs(neighbors_rank):
	neighbors = neighbors_rank[1][0]
	rank = float(neighbors_rank[1][1])
	num_neighbors =len(neighbors)
	for n in neighbors:
		yield (n,rank /num_neighbors)

input_lines=spark.read.text("/home/hadoop/Desktop/Assignment-2/BatsmanRankTestData.txt").rdd.map(lambda r: r[0])
lines=input_lines.map(lambda x:x.split(",")).cache()
ranks=lines.map(lambda x:(x[0],float(x[2])/float(x[3]))).reduceByKey(add)
links=lines.map(lambda x:(x[0],x[1])).groupByKey().map(lambda x:(x[0],list(x[1])))
weight=80
while 1:
	old_rank=ranks
	contribs=links.join(ranks).flatMap(lambda x: computeContribs(x))
	ranks = contribs.reduceByKey(add).mapValues(lambda rank: float(rank)*weight/100.0 + (100-weight)/100.0)
	if(checkConvergence(old_rank,ranks)):
		break 

print(ranks.collect())
"""
