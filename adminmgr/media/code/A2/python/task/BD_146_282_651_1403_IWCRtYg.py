from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession

#Here we are taking each line and returning the Bowler-Batsman link pairs
def parseNeighbors(data):
    parts = data.split(",")
    return parts[0], parts[1]
#Here we are computing individual batting averages for each batsman
def computeBattingAvg(x):
    parts=x.split(",")
    return parts[1],(float(parts[2])/float(parts[3]))
#Here we are computing the contributions of each player with the rank parsed
def computeContribs(players, rank):
     num_players = len(players)
     for player in players:
           yield (player,rank/num_players)
#Here we are checking for condition provided as a parameter if i/p=0  then default weight is 80/20 and otherwise
def computeWt(x):
    if x is 0:
        wt=0.80
    else:
        wt=float(x)/100
    return wt
#Some Initialisations:         
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: playerrank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPlayerRankings")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0]) #Reading the file from std input
    links = lines.map(lambda players: parseNeighbors(players)).distinct().groupByKey() #Getting links from bowler:[batsman1,batsman2..]

    rank_mid=lines.map(lambda x: computeBattingAvg(x))#Finding Batting average
    ranks=rank_mid.reduceByKey(add)#Calculating the sum of the batting averages
    for (player, rank) in ranks.collect():
        rank=max(rank,1.0) #Keeping a check of the condition given that initial rank must be 1.0 or >1.0
    
    wt1=computeWt(int(sys.argv[3]))#Computing weight based on std input
    wt2=1-wt1
    
    if int(sys.argv[2]) is 0:#If convergence
        cv=True
        count_of_iter=0#keeing count of iterations
        while cv:
            count_of_iter+=1
            contribs = links.join(ranks).flatMap(lambda player_rank: computeContribs(player_rank[1][0], player_rank[1][1]))#checking intial contributions of each batsman with corresponding ranks
            prevRank=list(ranks.collect())#keeping a check on prevranks to satisfy convergence condition of <0.0001
            ranks_new = contribs.reduceByKey(add).mapValues(lambda rank: rank * wt1 + wt2)#getting new ranks of the players
            inter_rank=list(ranks_new.collect())
            i=0
            while(i<len(prevRank)):
                if(abs(float(inter_rank[i][1])-float(prevRank[i][1])) < 0.0001): #Convergence Condition
                    i+=1
                    continue
                else:
                    break
            if(i is not len(prevRank)):
                ranks=ranks_new
            else:
                cv=False
    else:        
        for iteration in range(int(sys.argv[2])):#If input is iterations
            contribs = links.join(ranks).flatMap(lambda player_rank: computeContribs(player_rank[1][0], player_rank[1][1])) 
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * wt1 + wt2)
    ranksSorted=ranks.sortBy(lambda x: (-x[1],x[0]))#Sorting the ranks based on descending order of ranks and if they're the same then based on lexicographic ordering of the names
    for (player, rank) in ranksSorted.collect():
        print("%s,%.12f" % (player,round(rank,12)))
    spark.stop()      