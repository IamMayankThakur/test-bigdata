from __future__ import print_function
import re
import sys
from operator import add
from pyspark.sql import SparkSession

def calculate_contribs(urls,rank):
   num_urls = len(urls)
   for url in urls:
      yield (url,rank / num_urls)

def absDifference(rank,rank1):
  for i in range(len(rank)):
    if(abs(rank[i][1]-rank1[i][1])>0.0001):
      return 0
  return 1

def fy(a,b):
  return a,b

def scanNeighbours(urls,y):
  parts = re.split(r',', urls)
  if y==1:
    return parts[0],(parts[2],parts[3])#batsman,(wickets,balls)
  elif y==2:
    return parts[0],parts[1]

def bowlingAverage(a):
  s=0
  for i in a:
    s=s+float(i[0])/float(i[1])
  return max(s,1)

def sorting(rank):
  return sorted(rank,key=lambda n:(-n[1],n[0]))

if __name__ == "__main__":
  if len(sys.argv)!=4:
    print("Usage: pagerank <file> <iterations>", file=sys.stderr)
    sys.exit(-1)

  spark = SparkSession\
    .builder\
    .appName("PythonPageRank")\
    .getOrCreate()

  lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
  
  links = lines.map(lambda player:scanNeighbours(player,1)).distinct().groupByKey().cache()
  
  l = links.collect()
  
  ranks = links.map(lambda player_neighbours:(player_neighbours[0],bowlingAverage(player_neighbours[1])))#bowlers,rank
  
  new_links = lines.map(lambda players: scanNeighbours(players,2)).groupByKey().cache()

  contribs = new_links.join(ranks).flatMap(lambda player_rank: calculate_contribs(player_rank[1][0],player_rank[1][1]))
  
  if((float(sys.argv[3])/100)!=0.0):
    weight = float(sys.argv[3])/100
  else:
    weight = 0.8

  ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + 1-weight)

  flag=5
  if(int(sys.argv[2])==0):
    while(flag!=0):
      contribs = new_links.join(ranks).flatMap(lambda player_rank: calculate_contribs(player_rank[1][0], player_rank[1][1]))
      new_ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + 1-weight)
      output = sorting(ranks.collect())
      new_output = sorting(new_ranks.collect())
      if(absDifference(new_output,output)):
        flag=0
      ranks=new_ranks
  
  else:
    for iteration in range(int(sys.argv[2])):
      contribs = new_links.join(ranks).flatMap(lambda player_rank: calculate_contribs(player_rank[1][0], player_rank[1][1]))
      ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + 1-weight)
    output = sorting(ranks.collect())

  for(link,rank) in output:
    g = float("{0:.12f}".format(rank))
    print("%s,%s"%(link,g))
  spark.stop() 
