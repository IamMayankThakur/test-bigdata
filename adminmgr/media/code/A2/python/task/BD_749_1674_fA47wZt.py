from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession
from pyspark import SparkContext


def parsebowlers(rows):
	vals=rows.split(",")
	bowler=vals[0]
	batsman=vals[1]
	wickets=float(vals[2])
	deliveries=float(vals[3])
	avg=wickets/deliveries
	return avg

def parsebats(rows):
	vals=rows.split(",")
	bowler=vals[0]
	batsman=vals[1]

	return batsman

def bowler(rows):
	vals=rows.split(",")
	bowler=vals[0]
	return bowler

def comp(url):
	if url<1:
		return 1
	else:
		return url

def computeContribs(urls,rank):
	num_urls=len(urls)
	for url in urls:
		yield(url,float(rank/num_urls))

def converged(ranks,newranks,x,y):
	for i in range(len(newranks.collect())):
		ranks=newranks
		newranks=newranks.map(lambda m:(m[1]*x+y if m[1]-rank[i][1]>0.001 else m[1]))
	return (newranks.collect())

if __name__ == "__main__":
	
	#change -- file should not be read from stdin i guess
	if len(sys.argv)!=4:
		print("Usage pagerank <py file> <textfile> <iterations> <weight>",file=sys.stderr)
		sys.exit(-1)

	#initialize spark context
	spark=SparkSession.builder.appName("PageRank").getOrCreate()

	#sc=SparkContext(appName="bowl")

	#load input file
	lines=spark.read.text(sys.argv[1])

	#extract individual rows from the textfile
	lines=lines.rdd.map(lambda r: r[0])


	links=lines.map(lambda urls:(bowler(urls),parsebats(urls))).distinct().groupByKey().cache()

	
	links2=lines.map(lambda urls:(parsebats(urls),parsebowlers(urls)))

	avg=links2.reduceByKey(add)
	
	ranks=avg.map(lambda url:(url[0],comp(url[1])))
	

	num_of_iter=0
	newranks=ranks
	n=len(ranks.collect())
	if(int(sys.argv[2])>0):
		for iteration in range(int(sys.argv[2])):
			contribs = links.join(avg)
			contribs=contribs.flatMap(lambda url_rank:computeContribs(url_rank[1][0],url_rank[1][1]))
			if int(sys.argv[3])==0:
				x=0.8
				y=0.2
				ranks=contribs.reduceByKey(add).mapValues(lambda rank:rank*x+y)
			else:
				x=int(sys.argv[3])/100
				y=1-int(sys.argv[3])/100
				ranks=contribs.reduceByKey(add).mapValues(lambda rank:rank*x+y)

	
		
	else:
		while(True):
			
			contribs=links.join(ranks)
			contribs=contribs.flatMap(lambda url_rank:computeContribs(url_rank[1][0],url_rank[1][1]))    
			ranks=newranks
			if int(sys.argv[3])==0:
				x=0.8
				y=0.2
				ranks=contribs.reduceByKey(add).mapValues(lambda rank:(rank*x)+y)
			else:
				x=int(sys.argv[3])/100
				y=1-int(sys.argv[3])/100
				ranks=contribs.reduceByKey(add).mapValues(lambda rank:rank*x+y)
			newranks=newranks.sortBy(lambda x:x[0],True)
			ranks=ranks.sortBy(lambda x:x[0],True)
			flag=False

			for i,j in zip(newranks.collect(),ranks.collect()):
				if(abs(float(j[1])-float(i[1]))>=0.0001):
					flag=True
			
			if(flag==False):
				break;
			else:
				newranks=ranks
			


	#result=sorted(newranks.collect(),key=lambda x:-x[1])
	result=newranks.sortBy(lambda x:x[1],False)



	for i,j in result.collect():
		print("%s,%.12f" % (i,j))


spark.stop()
