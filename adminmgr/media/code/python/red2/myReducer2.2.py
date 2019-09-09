#!/usr/bin/python3
import sys

#import fileinput
from operator import itemgetter

myDictionary = {}
myList=[]

for line in sys.stdin:
    line = line.strip()
    line = line.split("\t")
    batsman, bowler, wickets = line[0], line[1], int(line[2])

    if batsman not in myDictionary:
        myDictionary[batsman] = {}
        myDictionary[batsman][bowler] = [0,0]    # [Wickets, Number of Balls Faced]


    elif batsman in myDictionary:
        if bowler not in myDictionary[batsman]:
            myDictionary[batsman][bowler] = [0,0]

    myDictionary[batsman][bowler][0] += wickets
    myDictionary[batsman][bowler][1] += 1


for batsman in list(myDictionary):                # Remove batsman-bowler pair who has less than 5 deliveries in between.
    for bowler in list(myDictionary[batsman]):
        if myDictionary[batsman][bowler][1] <= 5:
            del myDictionary[batsman][bowler]


for batsman in myDictionary:
    tempList = list(myDictionary[batsman].keys())

    if len(tempList) > 0:
        for bowler in myDictionary[batsman]:
            myList.append((batsman, bowler, myDictionary[batsman][bowler][0], myDictionary[batsman][bowler][1]))

#newList = sorted(myList, key=itemgetter(1))
newList = sorted(myList, key=itemgetter(0))
newList = sorted(newList, key=itemgetter(3))
newList = sorted(newList, key=itemgetter(2), reverse=True)


for (batsman, bowler, wickets, deliveries) in newList:
    print(batsman + ',' + bowler + ',' + str(wickets) + ',' + str(deliveries))
