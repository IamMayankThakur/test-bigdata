from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def contribution_function(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)



def compute(key,val):
    if(val>1):
        return key,val
    return key,1


def frstFunction(urls):
    parts_array = re.split(r',', urls)
    return parts_array[0],round(int(parts_array[2])/int(parts_array[3]),12)



def secondFunction(urls):
    parts_array = re.split(r',',urls)
    return parts_array[0],parts_array[1]


abc=0


def converge(val1,val2):
    if((int(val1)-int(val2))>0.0001):
        return true



if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)


    # Initialize the spark context.
    spark_assignment = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
    
    lines = spark_assignment.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    weight = int(sys.argv[3])/100
    iterations = int(sys.argv[2])
    


   
    if(weight==0):
        weight=0.8
    links2 = lines.map(lambda urls: frstFunction(urls)).groupByKey().mapValues(sum).cache()
    ranks=links2.map(lambda x:compute(x[0],x[1]))
    links1=lines.map(lambda urls: secondFunction(urls)).groupByKey().cache()
    x=1
    count=0#For calculating the total number of iterations in case of convergence

    #First iteration to have a value that can be compared in convergence
    contribs = links1.join(ranks).flatMap(lambda url_urls_rank: contribution_function(url_urls_rank[1][0], url_urls_rank[1][1]))
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + 1-weight)
    #given iterations as an argument the for loop runs till that value 

    if(iterations!=0):
        for iteration in range(int(sys.argv[2])):
            contribs = links1.join(ranks).flatMap(lambda url_urls_rank: contribution_function(url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: round(rank * weight + 1-weight,12))
        x=-2
        first_result=sorted(ranks.collect(),key=lambda a:(-a[1],a[0]))

    #Given 0 iterations as an arguement
    while(x>0):
        contribs = links1.join(ranks).flatMap(lambda url_urls_rank: contribution_function(url_urls_rank[1][0], url_urls_rank[1][1]))
        ranks_sec = contribs.reduceByKey(add).mapValues(lambda rank: round(rank * weight + 1-weight,12))
        count=count+1
        first_result=sorted(ranks.collect(),key=lambda a:(-a[1],a[0]))
        result1=sorted(ranks_sec.collect(),key=lambda a:(-a[1],a[0]))
        ranks=ranks_sec#abs function is used to get absolute value of the difference
        if(abs(result1[0][1]-first_result[0][1])<0.00001):
            x=-1            



    for (link, rank) in first_result:
        print("%s,%.12f" % (link, rank))
    #print("%d number of iterations happening"%(count))
    spark_assignment.stop()
