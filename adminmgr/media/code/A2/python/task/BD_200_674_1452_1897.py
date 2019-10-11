from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession



def computeContribs(x,urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url,rank / num_urls)



if __name__ == "__main__":

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    if(float(sys.argv[3])==0):
        weight = 0.8
    else:
        weight = float(float(sys.argv[3])/100)
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda x: (x.split(",")[0],x.split(",")[1])).distinct().groupByKey().cache()

    ranks1 = lines.map(lambda x:( x.split(",")[1],float(x.split(",")[2])/float(x.split(",")[3]))).distinct().groupByKey().cache()
    ranks3 = ranks1.map(lambda x:(x[0],max(sum(x[1]),1.00)))

    if(int(sys.argv[2])!=0):
        for i in range(int(sys.argv[2])):
            contribs = links.join(ranks3).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[0],url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks3 = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1.00-weight))
    else:
        stop = 0
        while(stop==0):
            count1 = 0
            prev_ranks = ranks3
            contribs = links.join(ranks3).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[0],url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks3 = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1.00-weight))
            new_ranks = prev_ranks.join(ranks3)
            for i in new_ranks.collect():
                if(round(i[1][0],4)==round(i[1][1],4)):
                    count1 = count1+1
            if(count1==ranks3.count()):
                print(count1,ranks3.count())
                stop = 1 
            #print(stop)

    for (x,y) in ranks3.sortBy(lambda x:(-x[1],x[0])).collect():
        print("%s,%.12f"%(x,y))

    spark.stop()

