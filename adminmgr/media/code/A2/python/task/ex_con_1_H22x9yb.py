from __future__ import print_function
import re
import sys
from operator import add
from pyspark.sql import SparkSession


def calculate_contributions(bowlers, rank):
    number_of_bowlers = len(bowlers)
    for bowler in bowlers:
        yield (bowler, rank / num_of_bowlers)

def calculate(key, value):
    if(value > 1):
        return key, value
    return key, 1


def first_function(bowlers):
    segments = re.split(r',', bowlers)
    return segments[0], int(segments[2])/int(segments[3])



def second_function(bowlers):
    segments = re.split(r',', bowlers)
    return segments[0], segments[1]



def convergence(num_1, num_2):
    if((int(num_1) - int(num_2)) > 0.0001):
        return True



if(__name__ == "__main__"):
    if(len(sys.argv) != 4):
        print("Usage: spark-submit <pythonfilepath> <file> <iterations> <weights>", file = sys.stderr)
        sys.exit(-1)


    # Initialize the spark context.
    
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
    
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    weight = int(sys.argv[3])/100
    loops = int(sys.argv[2])
    
    if(weight == 0):
        weight = 0.8
    links_2_rdd = lines.map(lambda bowlers: first_function(bowlers)).groupByKey().mapValues(sum).cache()
    ranks = links_2_rdd.map(lambda x: calculate(x[0], x[1]))
    links_1_rdd = lines.map(lambda bowlers: second_function(bowlers)).groupByKey().cache()
    cond = 1
    count = 0
    #For calculating the total number of iterations in case of convergence

    
    #First iteration to have a value that can be compared in convergence
    
    _contributions = links_1_rdd.join(ranks).flatMap(lambda bowler_bowlers_rank: calculate_contributions(bowler_bowlers_rank[1][0], bowler_bowlers_rank[1][1]))
    ranks = _contributions.reduceByKey(add).mapValues(lambda rank: rank * weight + 1-weight)
    
    #given iterations as an argument the for loop runs till that value 

    if(loops != 0):
        for i in range(int(sys.argv[2])):
            _contributions = links_1_rdd.join(ranks).flatMap(lambda bowler_bowlers_rank: calculate_contributions(bowler_bowlers_rank[1][0], bowler_bowlers_rank[1][1]))
            ranks = _contributions.reduceByKey(add).mapValues(lambda rank: rank * weight + 1 - weight)
    
        cond = 0
        result=sorted(ranks.collect(),key=lambda x:(-x[1], x[0]))

    
    #Given 0 iterations as an arguement
    while(cond > 0):
        _contributions = links_1_rdd.join(ranks).flatMap(lambda bowler_bowlers_rank: calculate_contributions(bowler_bowlers_rank[1][0], bowler_bowlers_rank[1][1]))
        rank_sec = _contributions.reduceByKey(add).mapValues(lambda rank: rank * weight + 1 - weight)
        count += 1
    
        res = sorted(ranks.collect(),key = lambda x:(-x[1],x[0]))
        res_alt = sorted(rank_sec.collect(),key = lambda x:(-x[1],x[0]))
        ranks = rank_sec        #abs function is used to get absolute value of the difference
    
        if(abs(res_alt[0][1] - res[0][1]) < 0.00001):
            cond = 0            



    for (link, rank) in result:
        print(str(link) + "," + str(rank))
    spark.stop()
