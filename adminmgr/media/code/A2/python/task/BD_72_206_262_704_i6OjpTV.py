import re
import sys
from operator import add
from pyspark.sql import SparkSession
import os

# os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3.6"
old_ranks = list()
ranks = list()

def not_converged():
    return old_ranks.join(ranks).map(lambda x : abs(x[1][0] - x[1][1]) > 0.0001).reduce(lambda x, y : x or y)     #return 1 if any have not converged

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield(url, rank / num_urls)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BowlerRank").getOrCreate()
    # lines = spark.read.text("BowlerRankTestData.txt").rdd.map(lambda r : r[0]).map(lambda urls : re.split(',+', urls))
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r : r[0]).map(lambda urls : re.split(',+', urls))

    bowlingrates = lines.map(lambda x : (x[0], int(x[2]) / int(x[3])))
    cumulativerates = bowlingrates.reduceByKey(lambda x, y : x + y)
    ranks = cumulativerates.map(lambda x : (x[0], max(x[1], 1)))
    
    links = lines.map(lambda combo : (combo[0], combo[1])).distinct().groupByKey().cache()
    n_iters = int(sys.argv[2])
    calc_weight = int(sys.argv[3])
    
    calc_weight = (calc_weight if calc_weight > 0 else 80) / 100
    def_weight = 1 - calc_weight
    
    iter_count = 0
    notconv = 1
    while(notconv and ((iter_count < n_iters) if n_iters > 0 else True)):
        contribs = links.join(ranks).flatMap(lambda person_opps_rank : computeContribs(person_opps_rank[1][0], person_opps_rank[1][1]))
        old_ranks = ranks
        ranks = contribs.reduceByKey(add).mapValues(lambda rank : rank * calc_weight + def_weight)
        notconv = not_converged()
        iter_count += 1
        
    for (player, rank) in sorted(ranks.collect(), key=lambda x : (-x[1], x[0])):
        print("%s,%s" % (player, round(rank, 12)))
    # print("Number of iterations", max(iter_count, n_iters))

    spark.stop()
