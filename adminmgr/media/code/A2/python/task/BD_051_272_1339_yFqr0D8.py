from __future__ import print_function
import re
import sys
from operator import add
from pyspark.sql import *
from pyspark.sql import SparkSession

def rankContribs(urls, rank):
	num_urls = len(urls)
	for url in urls:
		yield (url, rank / num_urls)

		
#def bowlingaverage(wickets,deliveries):
#	lines=x.split(',')
#	wickets=lines[2]
#	deliveries=lines[3]
#	bowlingaverage=float(wickets/deliveries)
#	return bowlingaverage

def keyvaluepair(urls):
	parts = re.split(r',',urls)
	return parts[0],parts[1]


def bowlavg(urls):
	parts=re.split(r',',urls)
	#batsman=parts[0]
	#bowler=parts[1]
	#wickets=parts[2]
	#deliveries=parts[3]
	return parts[1],float(parts[2])/float(parts[3])
	
	
if __name__ == "__main__":
	if len(sys.argv) != 4:
		sys.exit(-1)
    # Initialize the spark context.
	spark = SparkSession\
		.builder\
		.appName("BowlerRank")\
		.getOrCreate()
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	#print(lines.collect())
	links1=lines.map(lambda urls:bowlavg(urls)).distinct().groupByKey().mapValues(sum).cache()
	#print(links1.collect())
	ranks = links1.mapValues(lambda x:max(x,1.0))
	#print(links2.collect())
	links3=lines.map(lambda urls:keyvaluepair(urls)).distinct().groupByKey().cache()
	
	contribs = links3.join(ranks).flatMap(lambda url_urls_rank: rankContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
	#print(contribs.collect())
	ranks2=contribs.reduceByKey(add).mapValues(lambda x:x * 0.8+ 0.2)
	#print(ranks.collect())

	i=0	
	iterations = 0
	if(int(sys.argv[2])==0):
		if(int(sys.argv[3])==0):
			#ranks2 = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
			x=80
		else :

			x=int(sys.argv[3])
			
		flag=1
		while(flag):
			
			contribs = links3.join(ranks2).flatMap(lambda url_urls_rank: rankContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

			ranks2 = contribs.reduceByKey(add).mapValues(lambda rank: rank * (float(x)/100.00) + 1-(float(x)/100.00))

			newrank=ranks.join(ranks2)

			newrank1 = newrank.map(lambda x : x[1][0] - x[1][1])
			l = newrank1.collect()
			flag=0
			for i in l:
				if(abs(i)>0.0001):
					flag=1

			ranks=ranks2
			#iterations = iterations + 1     
	else:
		if(int(sys.argv[3])==0):
				#ranks2 = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.8 + 0.2)
			x=80
		else :
			x=int(sys.argv[3])
					
		for iteration in range(int(sys.argv[2])):
			contribs = links3.join(ranks).flatMap(lambda url_urls_rank: rankContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

			ranks2 = contribs.reduceByKey(add).mapValues(lambda rank: rank * (float(x)/100.00) + 1-(float(x)/100.00))
			ranks = ranks2	


	ranks=ranks2.sortByKey().sortBy(lambda a :(-a[1]))
	for (link, ranks) in ranks.collect():
		print("%s,%.12f" % (link, float(ranks)))
	#print(iterations)
	spark.stop()
