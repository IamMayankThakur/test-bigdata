from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def parseNeighbors(data):
    parts = data.split(",")
    return parts[0], parts[1]

def computeBattingAvg(x):
    parts=x.split(",")
    return parts[1],(float(parts[2])/float(parts[3]))
    
def computeContribs(players, rank):
     num_players = len(players)
     for player in players:
           yield (player,rank/num_players)

def computeWt(x):
    if x is 0:
        wt=0.80
    else:
        wt=float(x)/100
    return wt
                
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: playerrank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPlayerRankings")\
        .getOrCreate()
        
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    links = lines.map(lambda players: parseNeighbors(players)).distinct().groupByKey()

    rank_mid=lines.map(lambda x: computeBattingAvg(x))
    ranks=rank_mid.groupByKey().mapValues(sum)
    for (player, rank) in ranks.collect():
        rank=max(rank,1.0)
    
    wt1=computeWt(int(sys.argv[3]))
    wt2=1-wt1
    
    if int(sys.argv[2]) is 0:
        cv=True
        count_of_iter=0
        while cv:
            count_of_iter+=1
            contribs = links.join(ranks).flatMap(lambda player_rank: computeContribs(player_rank[1][0], player_rank[1][1]))
            prevRank=list(ranks.collect())
            ranks_new = contribs.reduceByKey(add).mapValues(lambda rank: rank * wt1 + wt2)
            inter_rank=list(ranks_new.collect())
            i=0
            while(i<len(prevRank)):
                if(abs(float(inter_rank[i][1])-float(prevRank[i][1])) < 0.0001):
                    i+=1
                    continue
                else:
                    break
            if(i is len(prevRank)):
                cv=False
            else:
                ranks=ranks_new
    else:        
        for iteration in range(int(sys.argv[2])):
            contribs = links.join(ranks).flatMap(lambda player_rank: computeContribs(player_rank[1][0], player_rank[1][1])) 
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * wt1 + wt2)
    ranksSorted=ranks.sortBy(lambda x: (-x[1],x[0]))
    for (player, rank) in ranksSorted.collect():
        print("%s,%.12f" % (player,round(rank,12)))
    spark.stop()      