#!/usr/bin/python3
import sys
from operator import itemgetter

myDictionary = {}
myList=[]


for line in sys.stdin:
    line = line.strip()
    line = line.split("\t")
    batsman = line[0]
    bowler = line[1]
    runs = int(line[2])

    if bowler not in myDictionary:
        myDictionary[bowler] = {}
        myDictionary[bowler][batsman] = [0,0]    # [Runs, Number of Balls Faced]

    elif bowler in myDictionary:
        if batsman not in myDictionary[bowler]:
            myDictionary[bowler][batsman] = [0,0]

    myDictionary[bowler][batsman][0] += runs
    myDictionary[bowler][batsman][1] += 1


for bowler in list(myDictionary):                # Remove batsman-bowler pair who has less than 5 deliveries in between.
    for batsman in list(myDictionary[bowler]):
        if myDictionary[bowler][batsman][1] <= 5:
            del myDictionary[bowler][batsman]


for bowler in myDictionary:
    tempList = list(myDictionary[bowler].keys())

    if len(tempList) > 0:
        for batsman in myDictionary[bowler]:
            myList.append((bowler,batsman, myDictionary[bowler][batsman][0], myDictionary[bowler][batsman][1]))

newList = sorted(myList, key=itemgetter(0))
newList = sorted(newList, key=itemgetter(3))
newList = sorted(newList, key=itemgetter(2), reverse=True)


for (bowler, batsman, wickets, deliveries) in newList:
    print(bowler + ',' + batsman + ',' + str(wickets) + ',' + str(deliveries))
