#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This is an example implementation of PageRank. For more conventional use,
Please refer to PageRank implementation provided by graphx
Example Usage:
bin/spark-submit examples/src/main/python/pagerank.py data/mllib/pagerank_data.txt 10
"""
from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, float(rank / num_urls))


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = urls.split(',')
    return parts[0], parts[1]

def parseNeighbors_rank(urls):
    #Hve to fix this splitting and converting to string
    """Parses a urls pair string into urls pair."""
    parts = urls.split(',')
    res=float(float(parts[2])/float(parts[3]))
    return parts[1], res

if __name__ == "__main__":
    

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    links = lines.map(lambda urls: parseNeighbors(urls)).groupByKey().cache()
    temp_rank= lines.map(lambda urls: parseNeighbors_rank(urls)).cache()
    consol= temp_rank.reduceByKey(add).cache()
    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = consol.map(lambda url_neighbors: (url_neighbors[0], max(url_neighbors[1],1)))
    tempo_ranks=ranks
    count=0
    # Calculates and updates URL ranks continuously using PageRank algorithm.
    # -------Change this to a while loop
    if(int(sys.argv[2])!=0):
        for iteration in range(int(sys.argv[2])):
            # Calculates URL contributions to the rank of other URLs.
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            if(int(sys.argv[3])==0):
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
            else:
                x=int(sys.argv[3])
                x=x/100
                y=1-x
                ranks= contribs.reduceByKey(add).mapValues(lambda rank: rank * x + y)
            # Re-calculates URL ranks based on neighbor contributions.
    else:
        while(True):
            count=count+1
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            if(int(sys.argv[3])==0):
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
            else:
                x=int(sys.argv[3])
                x=x/100
                y=1-x
                #Find out wether 0.2 to be multiplied with 1 or initial rank calculated
                ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * x + y)
            tmp_ranks=tempo_ranks.collect()
            tempo_ranks=tempo_ranks.sortBy(lambda x:x[0],True)
            #tempo_ranks=tempo_ranks.sortBy(lambda x:x[1],False)
            #ranked_t=ranks.collect()
            ranks=ranks.sortBy(lambda x:x[0],True)
            #ranks=ranks.sortBy(lambda x:x[1],False)

            flag=1
            run=1
	     
            for i,j in zip(tempo_ranks.collect(),ranks.collect()):
                #run=run+1
               	#if(run==9):
                    #break;
                #a=float(j[1])-float(i[1])
                #print(str(j[1])+"-"+str(i[1])+str(count),file=open("assg7.txt","a"))
                if(abs(float(j[1])-float(i[1]))>=0.0001):
                    flag=0
                  
            if(flag==1):
                break;
            else:
                tempo_ranks=ranks

    # Collects all URL ranks and dump them to console.
    ranks=ranks.sortBy(lambda x:x[0],True)
    ranks=ranks.sortBy(lambda x:x[1],False)
    for (link, rank) in ranks.collect():
        print("%s,%.12f,%d" % (link,float(rank),count))

    spark.stop()
#Problems:
#Are the ranks in the ranks rdd in the same order as that of the batsman in the other rdd. i.e since were calculation ranks, are the ranks linked to the players correctly
#Convergence comes in 2 or 1 iteration. is that correct.
#Should 0.2 i.e 1-x/100 be multiplied with the initial rank we calculated or just 1
