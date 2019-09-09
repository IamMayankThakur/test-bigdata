#!/usr/bin/python3
import sys

#import fileinput
from operator import itemgetter

myDictionary = {}
myList=[]

for line in sys.stdin:
    line = line.strip()
    line = line.split("\t")
    batsman, bowler, wicket = line[0], line[1], int(line[2])

    if (batsman, bowler) in myDictionary:
        myDictionary[(batsman, bowler)][0] += wicket
        myDictionary[(batsman, bowler)][1] += 1

    else:
        myDictionary[(batsman, bowler)] = [wicket,1]


for pair in list(myDictionary):                # Remove batsman-bowler pair who has less than 5 deliveries in between.
    if myDictionary[pair][1] <= 5:
        del myDictionary[pair]


for pair in myDictionary:
        myList.append((pair, myDictionary[pair][0], myDictionary[pair][1]))

newList = sorted(myList, key=lambda x: (-x[1], x[2], x[0]))
#newList = myList.sort(key=lambda x: (-x[1], x[2], x[0]))

for (pair, wickets, deliveries) in newList:
    print(pair[0] + ',' + pair[1] + ',' + str(wickets) + ',' + str(deliveries))
