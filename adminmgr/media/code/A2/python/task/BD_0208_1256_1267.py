from __future__ import print_function
import re
import sys
from operator import add
from pyspark.sql import *
from pyspark.sql import SparkSession

def rankContribs(urls, rank):
	for url in urls:
		yield (url, rank / len(urls))

def keyvaluepair(urls):
	parts = re.split(r',',urls)
	return parts[0],parts[1]

def bowlaverage(urls):
	parts = re.split(r',',urls)
	div = float(parts[2])/float(parts[3])
	return parts[1], div
	
	
if __name__ == "__main__":

	length = len(sys.argv)
	
	if (length != 4) == True:
		sys.exit(-1)

	spark = SparkSession\
		.builder\
		.appName("BowlerRank")\
		.getOrCreate()

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

	links1=lines.map(lambda urls:bowlaverage(urls)).distinct().groupByKey().mapValues(sum).cache()

	ranks = links1.mapValues(lambda x:max(x,1.0))

	links3=lines.map(lambda urls:keyvaluepair(urls)).distinct().groupByKey().cache()
	
	ranks2 = links3.join(ranks).flatMap(lambda url_urls_rank: rankContribs(url_urls_rank[1][0], url_urls_rank[1][1])).reduceByKey(add).mapValues(lambda x:x * 0.8+ 0.2)

	i=0	
	iterations = 0
	arg2 = int(sys.argv[2])
	arg3 = int(sys.argv[3])

	if(arg2==0)==True:
		if(arg3==0):
			x=80
		else :
			x=arg3

		flag=1

		while(flag==1) == True:
			
			cont = links3.join(ranks2)
			contribs = cont.flatMap(lambda url_urls_rank: rankContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

			r2 = contribs.reduceByKey(add)
			ranks2 = r2.mapValues(lambda rank: rank * (float(x)/100.00) + 1-(float(x)/100.00))

			newrank=ranks.join(ranks2)

			newrank1 = newrank.map(lambda x : x[1][0] - x[1][1])
			
			list1 = newrank1.collect()
			
			flag=0
			
			for i in list1:
				if((abs(i)>0.0001) == True):
					flag=1

			ranks=ranks2   
	else:
		if(arg3 == 0):
			x = 80
		else :
			x = arg3
					
		for iteration in range(arg2):
			contri = links3.join(ranks)
			contribs = contri.flatMap(lambda url_urls_rank: rankContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

			r2 = contribs.reduceByKey(add)
			ranks2 = r2.mapValues(lambda rank: rank * (float(x)/100.00) + 1-(float(x)/100.00))
			ranks = ranks2	

	sort1 = ranks2.sortByKey()
	ranks = sort1.sortBy(lambda a :(-a[1]))
	
	for (link, ranks) in ranks.collect():
		print("%s,%.12f" % (link, float(ranks)))

	spark.stop()
