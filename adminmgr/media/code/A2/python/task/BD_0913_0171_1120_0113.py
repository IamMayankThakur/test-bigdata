from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    print("The intermediates are %s" %(lines))

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    rank1 = lines.map(lambda z: (z.split(",")[1], z.split(",")[2]/z.split(",")[3]))
    r = rank1.reduceByKey((x,y) => x + y)
    weight = int(sys.argv[3]) * 0.01

    ranks = r.map(lambda urls: (x[0],max(x[1],1.0)))
    print(ranks.collect())
    if int(sys.argv[2]) == 0:
	flag = 1
	i=0 	#iterations
	while flag == 1 and i < 2000:
		i+=1
		pr=ranks
		contribs = links.join(ranks).flatMap(
			lambda urls_rank: computeContribs(urls_rank[1][0], urls_rank[1][1]))
		if (int(sys.argv[3])) == 0:
			ranks = contribs.reduceByKey(add).mapValues(lambda r: r * 0.80 + 0.2)
		else:
			ranks = contribs.reduceByKey(add).mapValues(lambda r: (r * weight) + (1-weight))
		flag = 0
    		d= pr.leftOuterJoin(ranks).map(lambda a:a[0],abs(a[1][0]-a[1][1]) if((a[1][0] is not None) and (a[1][1] is not None)))
		for x in d.collect():
			if x is not None:
				if x[1] >= 0.0001:
					flag = 1
    else:
	for i in range(int(sys.argv[2])):
		contribs = links.join(ranks).flatMap(
			lambda urls_rank: computeContribs(urls_rank[1][0], urls_rank[1][1]))

		if (int(sys.argv[3])) == 0:
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.2)
		else:
			ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank * weight) + (1-weight))
   RANK = ranks.sortBy(lambda z : (-z[1],z[0]))
		

   for (link, rank) in RANK.collect():
	print(link, "{:.12f}".format(rank),sep = ",") #Printed til 12 decimal places
   print()
   spark.stop()
