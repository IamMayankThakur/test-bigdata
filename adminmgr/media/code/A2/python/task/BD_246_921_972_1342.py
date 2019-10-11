from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url[0], rank / num_urls)


def parseNeighbors(pair):
    parts = re.split(r',', pair)
    return (parts[0], (parts[1], float(int(parts[2])/int(parts[3]))))


if __name__ == "__main__":

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    num_iter=int(sys.argv[2])

    if(int(sys.argv[3])==0):
        w=0.8
    else:
        w=float(int(sys.argv[3])/100)      # weight
    
    b=float(1-w)                       # bias

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda pairs: parseNeighbors(pairs)
                      ).distinct().groupByKey().cache()
    ranks = links.flatMap(lambda pairs: pairs[1]).reduceByKey(
        add).mapValues(lambda x: max(x, 1))


    
    #for i in range(len(links)):
    #    for j in range(len(links[i][1])):
    #        links[i][1][j]=links[i][1][j][0]

    #for i in ranks.collect():
    #    print(i)

    #ranks = links.map(lambda data: (data[0], max(rank_sum(data[1]),1.0)))

    x=1
    j=0

    while (x and (num_iter==0 or j<=num_iter)):
        contribs = links.join(ranks).flatMap(lambda x: computeContribs(x[1][0],x[1][1]))

        intermediate_rank = contribs.reduceByKey(add).mapValues(lambda rank: rank * w + b)
        #diff=list(map(lambda x,y:abs(x[1]-y[1]),ranks.collect(),interranks.collect()))
        #convergence=filter(lambda x: x<0.00001,diff)

        r = list(ranks.collect())
        i_r=list(intermediate_rank.collect())

        i=0
        while i<len(r):
            if(abs(r[i][1]-i_r[i][1])<0.0001):
                i+=1
                continue
            else:
                break

        if(i==len(r)):
            x=0

        else:
            ranks=intermediate_rank

        j+=1


    r=list(ranks.collect())
    r=sorted(r,key=lambda x:(-x[1],x[0]),reverse=False)

    #filename=open('/home/kishan/Desktop/out.txt','w')

    for (link, rank) in r:
        #filename.write("%s,%.12f\n" % (link, rank))
        print("%s,%.12f" % (link, rank))

    #print("NUMBER OF ITERATIONS:",j)

    #filename.close()
    spark.stop()
