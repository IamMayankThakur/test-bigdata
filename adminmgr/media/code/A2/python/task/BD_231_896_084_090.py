from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseString(string):
    parts = string.split(",")
    return parts[0],parts[1]

def getInitialRank(string):
    parts = string.split(",")
    return parts[0],(int(parts[2])/int(parts[3]))

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations> <weight>", file=sys.stderr)
        sys.exit(-1)

    weight = (0.8 if sys.argv[3] == '0' else int(sys.argv[3])/100)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PageRank_231_896_084_090")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda x: parseString(x)).groupByKey().cache()

    ranks = lines.map(lambda x: getInitialRank(x)).reduceByKey(add).map(lambda x: (x[0],x[1] if x[1] > 1 else 1.0))

    iteration = 0
    while True:
        old_ranks = ranks
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1-weight))

        if sys.argv[2] == '0':
            rank_diff = ranks.join(old_ranks).map(lambda x: (x[0],abs(x[1][0] - x[1][1]))).filter(lambda x: x[1] >= 0.0001).count()
            if rank_diff == 0:
                break
        else:
            iteration += 1
            if iteration >= int(sys.argv[2]):
                break

    final_rank = ranks.map(lambda x: (x[0],round(x[1],12))).sortBy(lambda x: (-x[1],x[0]))

    for (link,rank) in final_rank.collect():
        print("%s,%s" % (link,rank))

    spark.stop()