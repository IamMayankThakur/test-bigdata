from __future__ import print_function
import re
from pyspark import SparkContext
import sys
from operator import add
from pyspark.sql import SparkSession

#printing values from rdd
def print_values(ob):
    for i in ob.collect():
        print(i)

#returns batsman and bowler
def compute(line):
    prt=line.split(',')
    temp_bat = prt[0]
    temp_bow=prt[1]
    return (temp_bat,temp_bow)

#returns bowler and rank
def cal(x):
    prt=x.split(',')
    return prt[1], float(prt[2]) / float(prt[3])

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, float(rank) / num_urls)

def check(old,new):
    old.sort()
    new.sort()
    for i,j in zip(old,new):
        if (i[1]-j[1]) >= 0.0001:
            return 0
    return 1

def func(x):
    if x[1]<=1:
        return x[0],1.0
    else:
        return x[0],x[1]



if __name__=="__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank error <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkSession.builder.appName("please").getOrCreate()
    lines = sc.read.text(sys.argv[1]).rdd.map(lambda r:r[0])
    #batsman -> bowler
    links = lines.map(lambda x: compute(x)).distinct().groupByKey()
    #bowler -> rank
    links_2 = lines.map(lambda x:cal(x)).reduceByKey(add)
    #bolwer - > new rank (1)
    ranks = links_2.map(lambda x:func(x))

    con =False
    c=int(sys.argv[2])
    temp = int(sys.argv[3])/100
    if temp == 0:
        temp = 0.80

    while True:
        old = ranks
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], (url_urls_rank[1][1]) ))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank* temp + 1-temp)
        if c==0:
            if check(old.collect(),ranks.collect()):
                break
        else:
            c-=1
            if c==0:
                break


    ranks =ranks.sortBy(lambda x:-x[1])

    for (link, rank) in ranks.collect():
        print("%s,%s" % (link, format(rank,'.12f')))
    sc.stop()
