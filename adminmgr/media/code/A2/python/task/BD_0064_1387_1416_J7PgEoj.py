from __future__ import print_function
from decimal import *
import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(players, rank):
    num_players = len(players)
    for player in players:
        yield (player, rank / num_players)

def bow_batPair(lines):
    parts = re.split(r',', lines)
    return parts[0],parts[1]

def bow_batAvg(lines):
    parts = re.split(r',', lines)
    return parts[1],round(float(parts[2])/float(parts[3]),12)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
    
    data = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    player_pair = data.map(lambda lines: bow_batPair(lines)).distinct().groupByKey().cache()
    
    #to calculate default rank

    avg_player = data.map(lambda lines: bow_batAvg(lines)).distinct().groupByKey().cache()
    
    #def_ranks = links1.map(lambda x:(x[0],max(round(sum(x[1]),12),1.0)))
    #default rank calculated
    def_ranks = avg_player.map(lambda x:(x[0],max(sum(x[1]),1.0)))
    
    iterations = int(sys.argv[2])
    p = float(sys.argv[3])*0.01
    
    #for iterations not equal to 0
    if iterations!=0:
        
        for iteration in range(iterations):
            player_contribs = player_pair.join(def_ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            def_ranks = player_contribs.reduceByKey(add).mapValues(lambda rank: (rank *p  + 1-p))
    
    else:
      
        l=-1
        count = len(def_ranks.collect())
        while l!=count: 
            player_contribs = player_pair.join(def_ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            
            check1 = def_ranks.map(lambda x:x) 
            def_ranks = player_contribs.reduceByKey(add).mapValues(lambda rank:(rank * 0.80 + 0.20))   
            
            check2 = def_ranks.map(lambda x:x)
       
            diff=check1.join(check2).map(lambda x:abs(x[1][1]-x[1][0])).filter(lambda x:x<0.0001) 
            l = len(diff.collect())
    
    #sort the output
    sort = def_ranks.sortBy(lambda a: (-a[1],a[0]))

    #print the output
    for (player, rank) in sort.collect():
           print("%s,%s" % (player, rank))

    #stop the spark
    spark.stop()
