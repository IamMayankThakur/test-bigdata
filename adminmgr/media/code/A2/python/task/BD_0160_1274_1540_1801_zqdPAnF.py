from __future__ import print_function
#from pyspark.sql.functions import abs
import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    #parts = re.split(r'\s+', urls)
    parts = re.split(r',+', urls)
    bowl_avg=float(parts[2])/float(parts[3])
    #sec_part=str(parts[1])+','+str(bowl_avg)
    return parts[0],parts[1],bowl_avg

def monkey(x):
	n=len(x)
	sumx=0
	'''donk=[]
	for i in x:
		sum+=i[1]
		donk.append(i[0])'''
	sumx=x.flatMap(lambda record:record[1]).reduce(add)
	donk=x.flatMap(lambda record:record[0])
	if(sumx<1):
		sumx=1.0
	return donk,sumx
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    iter_count=int(sys.argv[2])
    weight=int(sys.argv[3])
   
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    #links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct()
     

    '''for (number, link) in links.collect():
        print("the array of %s is %s." % (number,list(link)))'''
   
    #rank = links.mapValues(lambda splt: re.split(',',splt))
	
    #rank = links.flatMapValues(monkey)
    bowlers = links.map(lambda record:(record[0],record[1])).groupByKey()
    sumx = links.map(lambda record:(record[0],record[2])).reduceByKey(add)
    ranks = sumx.map(lambda record:(record[0],record[1]) if (record[1]>1.0) else (record[0],1.0))
    #links = bowlers
    #ranks = sumx
    #links = rank.flatMapValues(lambda x:x[0:1])
    #ranks = rank.flatMapValues(lambda x:x[1:2])
    '''for (number, link) in ranks.collect():
        print("MAPVALUES %s is %s." % (number,link))'''
    ''' for l in rank.collect():
        print(list(l))'''
    #ranks = links.map(lambda url_neighbors: (url_neighbors[0], max(1.0,url_neighbours[1][1]))
    prev_rank=0
    
    iteration=1
    while(True):
        contribs = bowlers.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20 if weight==0 else rank * (weight/100) + (100-weight)/100 )
        '''for (link, rank) in ranks.collect():
            print('%s , %s' %(link,rank))'''
        if(iteration != 1 and iter_count == 0):
            ranks_join = ranks.join(prev_rank)
            '''for (link, rank) in ranks.collect():
                 print('AFTER JOIN:%s , %s' %(link,rank))'''
            #next_iter=ranks.filter(lambda r:((r[1][1]-r[1][0]) if(r[1][0]<r[1][1]) else (r[1][0]-r[1][1]))>0.0001).count()>0
            next_iter=ranks_join.filter(lambda r:abs(r[1][0]-r[1][1])>0.0001).count()>0
            '''for (link, rank) in ranks.collect():
                 print('AFTER MAP:%s , %s' %(link,rank))'''
            #ranks=ranks.map(lambda x:x[0]) 
            if(not next_iter):
                 print("GOING TO BREAK")                  
                 break
            
        else:
            if(iteration == iter_count):
                break
        iteration+=1
        prev_rank=ranks
        '''for (link, rank) in prev_rank.collect():
            print('prev_rank:%s , %s' %(link,rank))'''
    
     
    print('ITERATIONS:%s' % iteration)
    ranks=ranks.sortBy(lambda x: -x[1])
    for (link, rank) in ranks.collect():
        print('%s , %.12f' %(link,rank))
    #print(list(diff.collect()))
    spark.stop()

