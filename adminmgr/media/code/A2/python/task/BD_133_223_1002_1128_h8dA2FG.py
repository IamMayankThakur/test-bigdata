#Input File path: "hdfs://localhost:9000/assignment2/input/BowlerRankTestData.txt"
import os
import sys
from operator import add
from operator import sub
from pyspark import SparkContext

def getBowlAvg(data):
	record = data.split(',')
	return(record[0],(record[1],int(record[2])/int(record[3])))
	
def getAvgSum(data):
	sum=0
	for i in range(len(list(data[1]))):
		sum+=list(data[1])[i][1]
	return(sum)

def computeContrib(bowl_rank):
	neighbors = []
	for record in list(bowl_rank[1][0]):
		neighbors.append(record[0])
	rank = bowl_rank[1][1]
	num_neighbors = len(neighbors)
	for n in neighbors:
		yield(n,rank/num_neighbors)
		
def display(ranks):
	sorted_ranks = ranks.sortBy(lambda r:(-r[1],r[0]))
	for record in sorted_ranks.collect():
		print(record[0],"%.12f" % record[1],sep=',')
	

sc = SparkContext(appName="Bowler Page Rank")
lines = sc.textFile(sys.argv[1])				#get input data

#get bowling average of each record and group by Bowler's Name
links = lines.map(lambda record: getBowlAvg(record))
links = links.groupByKey().cache()

#set initial ranks for each bowler
ranks = links.map(lambda record: (record[0],max(getAvgSum(record),1)))


if((int(sys.argv[2])==0) and (int(sys.argv[3])==0)):		#converge with default weights
	ctr=1
	while(1):
		#print(ctr)
		contrib = links.join(ranks).flatMap(lambda bowl_rank: computeContrib(bowl_rank))
		new_ranks = contrib.reduceByKey(add).mapValues(lambda rank: rank*0.80+0.20) 
		#diff = list(map(lambda x,y: abs(x[1]-y[1]),new_ranks.collect(),ranks.collect()))
		join_ranks = new_ranks.join(ranks)
		diff = [abs(x[1][0]-x[1][1]) for x in join_ranks.collect() ]
		flag=1
		for i in diff:
			if(i>=0.0001):
				flag=0
		if(flag==1):		
			display(new_ranks)
			break
		else:
			ranks=new_ranks
			#ctr+=1
			
elif((int(sys.argv[3])==0) and (int(sys.argv[2])!=0)):			#n iterations with default weights
	for i in range(int(sys.argv[2])):
		contrib = links.join(ranks).flatMap(lambda bowl_rank: computeContrib(bowl_rank))
		ranks = contrib.reduceByKey(add).mapValues(lambda rank: rank*0.80+0.20)
	display(ranks)

elif((int(sys.argv[2])==0) and (int(sys.argv[3])!=0)):		#converge with specific weights
	while(1):
		contrib = links.join(ranks).flatMap(lambda bowl_rank: computeContrib(bowl_rank))
		a = int(sys.argv[3])/100
		b = (100-int(sys.argv[3]))/100
		new_ranks = contrib.reduceByKey(add).mapValues(lambda rank: rank*a+b)
		#diff = list(map(lambda x,y: abs(x[1]-y[1]),new_ranks.collect(),ranks.collect()))
		join_ranks = new_ranks.join(ranks)
		diff = [abs(x[1][0]-x[1][1]) for x in join_ranks.collect() ]
		flag=1
		for i in diff:
			if(i>=0.0001):
				flag=0
		if(flag==1):		
			display(new_ranks)
			break
		else:
			ranks=new_ranks
		

else:							#n iterations with specific weights
	for i in range(int(sys.argv[2])):
		contrib = links.join(ranks).flatMap(lambda bowl_rank: computeContrib(bowl_rank))
		a = int(sys.argv[3])/100
		b = (100-int(sys.argv[3]))/100
		ranks = contrib.reduceByKey(add).mapValues(lambda rank: rank*a+b)
	display(ranks)
	
	
	
