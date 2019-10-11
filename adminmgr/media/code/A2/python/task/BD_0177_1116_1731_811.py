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
    parts = re.split(r',', urls)
    if(flag__1==0):
        return parts[0], parts[1]
    else:
        return parts[0], int(parts[2])/int(parts[3])

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    flag__1=1
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    links1 = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().mapValues(sum).cache()
    ranks = links1.map(lambda x:(x[0],max(x[1],1)))
    flag__1=0
    links2 = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    if(int(sys.argv[2])==0):
        flag=0
        count=1
        flag2=0
        flag3=0
        flag4=0
        count_1=0
        while(flag2==0):
            for iteration in range(count):
                if(flag==1):
                    prev_ranks=ranks
                    flag3=1
                contribs = links2.join(ranks).flatMap(
                    lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
                if(int(sys.argv[3])==0):
                    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
                else:
                    num=int(sys.argv[3])
                    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank *(num/100) +(1-(num/100)) ) 
                flag=1
                count=count+1
            k=0
            if(flag4==0):
                for (link, rank) in ranks.collect():
                    count_1=count_1 +1
            flag4=1         
            for (link, rank) in ranks.collect():
                if(flag3==1):
                    for (link1, rank1) in prev_ranks.collect():
                        if(link1==link and rank1-rank>0):
                            if(rank1-rank<0.0001):
                                k=k+1
                        elif(link1==link and rank1-rank<=0):
                            if(rank-rank1<0.0001):
                                k=k+1
            if(k==count_1):
               # print(count)
                ranks=sorted(ranks.collect(),key=lambda sort: (-sort[1],sort[0]))
                for (link, rank) in ranks:
                    print(link,rank,sep=",")
                flag2=1
    else:
        for iteration in range(int(sys.argv[2])):
            contribs = links2.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            if(int(sys.argv[3])==0):
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
            else:
                num=int(sys.argv[3])
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank *(num/100) +(1-(num/100)) )

        ranks=sorted(ranks.collect(),key=lambda sort: (-sort[1],sort[0]))
        for (link, rank) in ranks:
            print(link,rank,sep=",")    
    spark.stop()

