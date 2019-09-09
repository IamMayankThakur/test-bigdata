#!/usr/bin/python3
import sys
from operator import itemgetter

myDictionary = {}
myList=[]
dismisalList = ['caught and bowled', 'caught', 'bowled', 'stumped', 'lbw', 'hit wicket', 'obstructing the field']

for line in sys.stdin:
    line = line.strip()
    line = line.split("\t")
    batsman = line[0]
    bowler = line[1]
    wicket = line[2]

    if batsman not in myDictionary:
        myDictionary[batsman] = {}
        myDictionary[batsman][bowler] = [0,0]    # [Wickets, Number of Balls Faced]

        if wicket in dismisalList:
            myDictionary[batsman][bowler][0] += 1

        myDictionary[batsman][bowler][1] += 1

    elif batsman in myDictionary:
        if bowler not in myDictionary[batsman]:
            myDictionary[batsman][bowler] = [0,0]

        elif wicket in dismisalList:
            myDictionary[batsman][bowler][0] += 1

        myDictionary[batsman][bowler][1] += 1


for batsman in list(myDictionary):                # Remove batsman-bowler pair who has less than 5 deliveries in between.
    for bowler in list(myDictionary[batsman]):
        if myDictionary[batsman][bowler][1] < 5:
            del myDictionary[batsman][bowler]


for batsman in myDictionary:
    tempBatsman = batsman
    tempList = list(myDictionary[batsman].keys())
    tempBowler = tempList[0]

    for bowler in myDictionary[batsman]:
        if(myDictionary[batsman][bowler][0] == myDictionary[batsman][tempBowler][0]):
            if(myDictionary[batsman][bowler][1] < myDictionary[batsman][tempBowler][1]):
                tempBowler = bowler

        elif(myDictionary[batsman][bowler][0] > myDictionary[batsman][tempBowler][0]):
            tempBowler = bowler

    myList.append((tempBatsman,tempBowler, myDictionary[batsman][tempBowler][0], myDictionary[batsman][tempBowler][1]))


newList = sorted(myList, key=itemgetter(2), reverse=True)

for (batsman, bowler, wickets, deliveries) in newList:
    print(batsman + ',' + bowler + ',' + str(wickets) + ',' + str(deliveries))
