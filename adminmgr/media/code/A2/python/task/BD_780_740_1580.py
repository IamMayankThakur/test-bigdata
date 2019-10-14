#!/usr/bin/python
from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession
from  pyspark.sql.functions import abs
from pyspark.sql.functions import desc


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)
        
def bowling_average(wkts, delv):
    bavg = float(wkts/delv)
    return bavg

def parseNeighbors(urls):
    parts = re.split(r',', urls)
    res = bowling_average(parts[2], parts[3])
    return parts[1], res 

'''def initialize(links):
    val=links.map(lambgroupByKey()
    
    return max(val, 1)'''


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)
# Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0]) #will return ((bow,bat,run,del),..)
    '''
    print(lines.collect())
    print("\n\n")'''

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()   #will return ((bow1,(bavg1,bavg2,bavg3)),(bow2,(bavg2,babg3))...)
    '''print("LINKS\n")
    #for i in links:
    print(links.collect())
    print("\n\n")'''

    ranks = links.map(lambda url_neighbors: (url_neighbors[0], max(sum(url_neighbors[1],1))) #will return ((bow1,6.890),(bow2,1.0)...)
    '''print("RANKS\n")
    print(ranks.collect())
    print("\n\n")'''


    '''#####Please read the comments to understand the logic######
        Try executing it and tell me the errors,I'm not really sure if the syntax is correct
        ###########okay bye ###################'''

    init=ranks                                         
    if !(int(sys.argv[2]==0)): #has to run till convergence if number of iteratins =0
    #TODO:convergence factor to be calculated by subtracting (pres rank - prev rank).If this value goes below 0.0001 for any bowler,break and return ranks,else repeat
    #
        while 1:#figure out this guy
            
            
            if (sys.argv[3]==0):#if weight is zero,take 80:20
                contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))#will return ((bow1,0.878)....)
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
                x = init.map(lambda var1: (var1[1])) #gets rank of prev ranks ex:(2.3,4.5....)
                y = ranks.map(lambda var2: (var2[1]))#pres ranks ex:(3.2,6.5,....)
                res1 = x.join(y) #joining both to get something like ((2.3,3.2),(4.5,6.5)...)
                diff = res1.map(lambda dif_var :abs(dif_var[0]-dif_var[1]))#gets the abs value of the difference ,too lazy to calculate
                
                
            else:
                contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))#will return ((bow1,0.878)....)
                v=sys.argv[3]/100
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * v + (1-v) )   #assigning the given weights
                x1 = init.map(lambda var11: (var11[1])) #gets rank of prev ranks ex:(2.3,4.5....)
                y1 = ranks.map(lambda var21: (var21[1]))#pres ranks ex:(3.2,6.5,....)
                res11 = x1.join(y1) #joining both to get something like ((2.3,3.2),(4.5,6.5)...)
                diff = res11.map(lambda dif_var1 :abs(dif_var1[0]-dif_var1[1]))#gets the abs value of the difference ,too lazy to calculate

            for i in diff: #if any difference is less than 0.0001 break 
                      if i<=0.0001:
                          break
                    
            init=ranks #reinitializing for the next iteration
                
                
            
                
            
    else:
        if (sys.argv[3]==0):       #3rd cmd line arg is the weight in percentage                                                    
            for iteration in range(int(sys.argv[2])):#no of iterations
                contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))#will return ((bow1,0.878)....)
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
        else:
            for iteration in range(int(sys.argv[2])):#no of iterations
                contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))#will return ((bow1,0.878)....)
                v=sys.argv[3]/100
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * v + (1-v)) #assigning the given weights
                                                            
    #let result be the end result with the final ranks and bowler name
    
    ranks=ranks.map(lambda (n1,n2):(n1,sorted(n2 ,key=lambda x:x[1],reverse=True)))
                      
    for bowler,rank in ranks.collect():
                      print(bowler,rank)





    spark.stop()
