from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession

def computeContribs(urls, a):
	num_urls = len(urls)
	for url in urls:
        	yield (url, a / num_urls)

def parseNeighbors(urls):
	splits = re.split(r',', urls)
	return parts[1],int(splits[2])/int(splits[3])
	

def parseNeighbors1(urls):
	splits = re.split(r',', urls)
	return splits[0], splits[1]

def converge(old_a,new_a):
	old=old_a.collect()
	new=new_a.collect()
	count=0
	has_converged = 1
	i = 0
	while i < len(new) and not has_converged:
		if (new[i][1]-old[i][1] < 0.0001):
			count+=1
			has_converged &= 1
		else:
			has_converged &= 0
		i += 1
	#print("converged : ",i)
	return has_converged

if __name__ == "__main__":
	#print("Start")
	if len(sys.argv) != 4:
        	print("Usage: pagea <file> <iterations>", file=sys.stderr)
        	sys.exit(-1)


# Initialize the spark context.
	spark = SparkSession\
		.builder\
		.appName("PythonPageRank")\
		.getOrCreate()
	#print("Read")
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	as = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().mapValues(sum).cache()
	as_new=as.mapValues(lambda x:max(x,1.0))
	bs = lines.map(lambda urls: parseNeighbors1(urls)).distinct().groupByKey().cache()

	weight=float(sys.argv[3])
	num_iter = 0
	#print("Begin")
	if(int(sys.argv[2]) == 0):
		old_a=None
		while True:
			num_iter+=1
			contribs = bs.join(as_new).flatMap(lambda url_urls_a: computeContribs(url_urls_a[1][0], url_urls_a[1][1]))
			old_a=as_new
			#print("check 1")
			as = contribs.reduceByKey(add).mapValues(lambda a: a * weight + 1-weight)
			new_a=as
			#print("check 2")
			#print(num_iter)
			if converge(old_a,new_a) :
				break
			#old_a=new_a
		#print("num_iter :",num_iter)

	elif(int(sys.argv[2]) > 0):
		for iteration in range(int(sys.argv[2])):
			contribs = bs.join(as).flatMap(lambda url_urls_a: computeContribs(url_urls_a[1][0], url_urls_a[1][1]))
			
	for (b, a) in a_sort.collect():
		print("%s,%s" % (ss, format(a,'.12gf')))

	spark.stop()
