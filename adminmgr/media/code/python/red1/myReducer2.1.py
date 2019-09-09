#!/usr/bin/python3
import sys

#import fileinput
from operator import itemgetter

myDictionary = {}
myList=[]
dismisalList = ['caught and bowled', 'caught', 'bowled', 'stumped', 'lbw', 'hit wicket', 'obstructing the field']
nonDismisalList = ['run out', 'retired hurt', '""']

for line in sys.stdin:
    line = line.strip()
    line = line.split("\t")
    batsman, bowler, wicket = line[0], line[1], line[2]

    if bowler not in myDictionary:
        myDictionary[bowler] = {}
        myDictionary[bowler][batsman] = [0,0]    # [Wickets, Number of Balls Faced]


    elif bowler in myDictionary:
        if batsman not in myDictionary[bowler]:
            myDictionary[bowler][batsman] = [0,0]

    if wicket not in nonDismisalList:
        myDictionary[bowler][batsman][0] += 1

    myDictionary[bowler][batsman][1] += 1


for bowler in list(myDictionary):                # Remove batsman-bowler pair who has less than 5 deliveries in between.
    for batsman in list(myDictionary[bowler]):
        if myDictionary[bowler][batsman][1] <= 5:
            del myDictionary[bowler][batsman]


for bowler in myDictionary:
    tempList = list(myDictionary[bowler].keys())

    if len(tempList) > 0:
        for batsman in myDictionary[bowler]:
            myList.append((batsman, bowler, myDictionary[bowler][batsman][0], myDictionary[bowler][batsman][1]))

#newList = sorted(myList, key=itemgetter(1))
newList = sorted(myList, key=itemgetter(0))
newList = sorted(newList, key=itemgetter(3))
newList = sorted(newList, key=itemgetter(2), reverse=True)


for (batsman, bowler, wickets, deliveries) in newList:
    print(batsman + ',' + bowler + ',' + str(wickets) + ',' + str(deliveries))
