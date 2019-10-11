from __future__ import print_function
import re
import sys
from operator import add
from pyspark.sql import *





def calcRank(BatBowl, rank):
    n = len(BatBowl)
    for i in BatBowl:
        yield (i, rank/n)



checking = 1

def batbowlKeyValue(x):
    lol = x.split(',')
    return lol[0],lol[1]



def batbowlRank(x):
    lol = x.split(',')
    return lol[1],float(lol[2])/float(lol[3])




if __name__ == "__main__" :

    if len(sys.argv) != 4:
        sys.exit(-1)


    spark = SparkSession\
        .builder\
        .appName("Bowlerrank")\
        .getOrCreate()



    lol = spark.read.text(sys.argv[1]).rdd.map(lambda x : x[0])

    lol2 = lol.map(lambda x: batbowlKeyValue(x)).distinct().groupByKey().cache()
    lol_temp = lol.map(lambda x: batbowlRank(x)).distinct().groupByKey().cache()

    bowr = lol_temp.map(lambda x : (x[0], max(sum(x[1]),1.00)))
    itcount = 0
    bowr_temp = bowr
    noi = int(sys.argv[2])

    if (noi <= 0) :
        while True:
            lol3 = lol2.join(bowr).flatMap(lambda x : calcRank(x[1][0], x[1][1]))
            perc = int(sys.argv[3])
            if(perc!=0):
                bowr = lol3.reduceByKey(add).mapValues(lambda deadpool : deadpool*(float(perc/100)) + 1-(float(perc/100)))
            else:
                bowr = lol3.reduceByKey(add).mapValues(lambda deadpool : deadpool*0.8 + 0.2)

            #for wolverine, iron_man in bowr.collect():
            #    print("%s has rank: %s." % (wolverine, iron_man))

            temp = bowr.join(bowr_temp)
            temp2 = temp.collect()
            flag = 0
            for i in temp2:
                if(round(i[1][0],4) == round(i[1][1],4)):
                    flag = flag + 1
                else:
                    break
            itcount = itcount + 1

            bowr_temp = bowr

            if flag==len(temp2):
                break



    else:
        t = int(sys.argv[2])
        for _ in range(t):
            lol3 = lol2.join(bowr).flatMap(lambda x : calcRank(x[1][0], x[1][1]))
            perc = int(sys.argv[3])
            if(perc!=0):
                bowr = lol3.reduceByKey(add).mapValues(lambda deadpool : deadpool*(float(perc)/100.00) + 1-(float(perc)/100.00))
            else:
                bowr = lol3.reduceByKey(add).mapValues(lambda deadpool : deadpool*0.8 + 0.2)




    bowr = bowr.sortBy(lambda x : (-x[1],x[0]))

    for wolverine, iron_man in bowr.collect():
        print("%s,%.12f" % (wolverine, iron_man))

    #print("...................................",itcount,"...............................................")


    spark.stop()

