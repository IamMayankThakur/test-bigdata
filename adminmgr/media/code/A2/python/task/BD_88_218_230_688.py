#! usr/bin/python3.5
from __future__ import print_function
import re
import sys
import argparse
from operator import add
import findspark
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from pyspark.sql.functions import greatest
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkContext
from decimal import Decimal

sc =SparkContext.getOrCreate()

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

if __name__ == "__main__":
    if len(sys.argv) != 4:
       print("Usage: batsman prowess <file> <iterations> <weight>", file=sys.stderr)
       sys.exit(-1)
   

# Initialize the spark context.
    sc =SparkContext.getOrCreate()
    spark=SparkSession(sc)

    lines = sc.textFile(sys.argv[1]).map(lambda x:x.split(","))
    lines2=lines.map(lambda y:(y[0],y[1]))
    lines3=lines.map(lambda y:(y[1],y[2],y[3]))

    bb=lines3.map(lambda x: (x[0],(float(x[1])/float(x[2]))))
    ranks=bb.reduceByKey(lambda x,y:x+y).map(lambda y:(y[0], max(1,y[1])))
    links = lines2.groupByKey().cache()

    if(int(sys.argv[3])==0):
        k=0.8
		#print("taking default sys arg")
    else:
        k=float(int(sys.argv[3])*0.01)
		#print("taking sysargv")
	
    if(int(sys.argv[2])==0):
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*k+(1-k))
        iteration=1
        while(1):
            flag=1
            copy=ranks
            contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*k+(1-k))
            combi=ranks.join(copy)
            for i in combi.collect():
                if(abs(float(i[1][1])-float(i[1][0]))>0.0001):
                    flag=0
            if (flag==1):
                break
            iteration=iteration+1
            #print(iteration)
    else:
        for iteration in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*k+(1-k))
	
    ranks_lexo=ranks.sortBy(lambda x:x[0],ascending=True)
    ranks_sorted=ranks_lexo.sortBy(lambda x:x[1],ascending=False)
    for (player,rank) in ranks_sorted.collect():
        print("%s,%s" % (player, round(Decimal(rank),12)))
	
    spark.stop()
