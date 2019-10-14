from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)



def compute(key,val):
    if(val>1):
        return key,val
    return key,1


def frstFunction(urls):
    parts = re.split(r',', urls)
    return parts[0],round(int(parts[2])/int(parts[3]),12)



def secondFunction(urls):
    parts = re.split(r',',urls)
    return parts[0],parts[1]



def converge(a,b):
    if((int(a)-int(b))>0.0001):
        return true



if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)


    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
    
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    percentage = int(sys.argv[3])/100
    iterations = int(sys.argv[2])
    


   
    if(percentage==0):
        percentage=0.8
    links2 = lines.map(lambda urls: frstFunction(urls)).groupByKey().mapValues(sum).cache()
    ranks=links2.map(lambda x:compute(x[0],x[1]))
    links1=lines.map(lambda urls: secondFunction(urls)).groupByKey().cache()
    x=1
    count=0#For calculating the total number of iterations in case of convergence

    #First iteration to have a value that can be compared in convergence
    contribs = links1.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * percentage + 1-percentage)
    #given iterations as an argument the for loop runs till that value 

    if(iterations!=0):
        for iteration in range(int(sys.argv[2])):
            contribs = links1.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: round(rank * percentage + 1-percentage,12))
        x=0
        result=sorted(ranks.collect(),key=lambda y:(-y[1],y[0]))

    #Given 0 iterations as an arguement
    while(x>0):
        contribs = links1.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        ranks_2 = contribs.reduceByKey(add).mapValues(lambda rank: round(rank * percentage + 1-percentage,12))
        count=count+1
        result=sorted(ranks.collect(),key=lambda y:(-y[1],y[0]))
        result1=sorted(ranks_2.collect(),key=lambda y:(-y[1],y[0]))
        ranks=ranks_2#abs function is used to get absolute value of the difference
        if(abs(result1[0][1]-result[0][1])<0.000001):
            x=0            



    for (link, rank) in result:
        print("%s,%.12f" % (link, rank))
    #print("%d number of iterations happening"%(count))
    spark.stop()
