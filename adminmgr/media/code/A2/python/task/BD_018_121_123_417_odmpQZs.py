#!/usr/bin/python3
from __future__ import print_function
from __future__ import division
import re
import sys
from operator import add
from pyspark.sql import SparkSession
from operator import itemgetter

#Weight updation
def computeContribs(urls, rank):
	num_urls = len(urls)
	for url in urls:
		yield (url,float(rank)/float(num_urls))

#link calculation
def parseNeighbors(urls):
	my_list = re.split(r',', urls)
	Batsman = my_list[0]
	Bowler = my_list[1]
	return Batsman,Bowler 

def calc_avg(lines):
	init_avg = []
	for line in lines.collect():
		my_list  = line.split(',')
		Batsman = my_list[0]
		Bowler = my_list[1]
		Delivery =my_list[3]
		Wicket = my_list[2]
		
		Delivery = int(Delivery)
		Wicket = int(Wicket)
		temp = list([Batsman,Bowler])
		average = float(Wicket)/float(Delivery)
		flg = 1

		if(init_avg==[]):
			init_avg.append([Bowler,average])
			flg = 0
		for i in init_avg:
			if(Bowler in i):
				i[1]+=average
				flg = 0
		if(flg == 1):
			init_avg.append([Bowler,average])

	return init_avg

#Rank updation (1 if average<0 else average)	
def func(lst,name):
	for i in lst:
		if(i[0]==name):
			if(i[1]>1):
				return i[1]
			return 1

if __name__ == "__main__":
	# Initialize the spark context.
	spark = SparkSession\
		.builder\
		.appName("PythonPageRank")\
		.getOrCreate()

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	
	init_avg = calc_avg(lines)
	avg = list()

	links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey()
	
	ranks = links.map(lambda url_neighbors: (url_neighbors[0],func(init_avg,url_neighbors[0])))

	prev = []
	
	for (i,j) in ranks.collect():
		prev.append(float(j))
	iter = True
	k=0
	#Iterate until convergence 
	while(k != len(prev)):

		contribs = links.join(ranks).flatMap(
		    lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
		
		ranks = contribs.reduceByKey(add).mapValues(lambda rank:rank * 0.80 + 0.20)

		cur = []
		k=0
		for (i,j) in ranks.collect():
			cur.append(float(j))
		prev.sort()
		cur.sort()
		#Check for convergence value
		for i in range(len(prev)):
			if(abs(prev[i]-cur[i])<0.0001):
				k+=1
			else:
				break
		prev = cur
		
	#print the output accordingly
	lst = []
	for (link, rank) in ranks.collect():
		avg.append([link,-1*rank])
	lst = sorted(avg, key=itemgetter(1,0))

	for i in lst:	
		print('%s,%.12f'%(i[0],-1*i[1]))
	spark.stop()
	
