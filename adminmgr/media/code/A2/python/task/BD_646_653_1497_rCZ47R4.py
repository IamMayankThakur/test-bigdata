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
    return parts[0],parts[1]

def bowlavg(urls):
    parts = re.split(r',', urls)
    return (parts[0],float(parts[2])/float(parts[3]))

def converge(old,new):
    joinedrdd=old.join(new).collect()
    print(joinedrdd)
    for i in joinedrdd:
        diff=i[1][0]-i[1][1]
        if diff >= 0.0001:
         return 1
    return 0
    



if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)
    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
   

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey()
    

    bowl=lines.map(lambda x: bowlavg(x)).distinct().reduceByKey(add)
 
    ranks = bowl.map(lambda url_neighbors: (url_neighbors[0], max(1,url_neighbors[1])))
   
    
    iterations=int(sys.argv[2])
    weights=int(sys.argv[3])/100
  
   
    if iterations > 0 :
      if weights == 0 :
        for iteration in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
      
            ranks=contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
      else:
        for iteration in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks=contribs.reduceByKey(add).mapValues(lambda rank: rank * weights + 1-weights)
      
    elif(iterations==0):
        if weights == 0:
                        oldv=ranks
                        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

                        ranks=contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
                        while(1):
                         if(converge(oldv,ranks)):
                           oldv=ranks

                           contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

                           ranks=contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
                        
                           
                         else:
                          break
        else:
                        oldv=ranks
                        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

                        ranks=contribs.reduceByKey(add).mapValues(lambda rank: rank * weights + 1-weights)
                        while(1):
                         if(converge(oldv,ranks)):
                           oldv=ranks

                           contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

                           ranks=contribs.reduceByKey(add).mapValues(lambda rank: rank * weights + 1-weights)
                        
                           
                         else:
                          break
         
          
      
    
    rankasc=ranks.sortBy(lambda x: x[0],ascending=True)
    finalranks=rankasc.sortBy(lambda x: x[1],ascending=False)    
    for (link, rank) in finalranks.collect():
       
        print("%s,%s" % (link, format(rank,'.12f')))

    spark.stop()
