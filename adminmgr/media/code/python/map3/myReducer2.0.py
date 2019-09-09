#!usr/bin/python3
import sys

#import fileinput
from operator import itemgetter

myDictionary = {}
myList=[]
dismisalList = ['caught and bowled', 'caught', 'bowled', 'stumped', 'lbw', 'hit wicket', 'obstructing the field']

for line in sys.stdin:
    line = line.strip()
    line = line.split("\t")
    batsman, bowler, wicket = line[0], line[1], line[2]

    if batsman not in myDictionary:
        myDictionary[batsman] = {}
        myDictionary[batsman][bowler] = [0,0]    # [Wickets, Number of Balls Faced]


    elif batsman in myDictionary:
        if bowler not in myDictionary[batsman]:
            myDictionary[batsman][bowler] = [0,0]

    if wicket in dismisalList:
        myDictionary[batsman][bowler][0] += 1

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

f = open('empty.txt', "w")

for (batsman, bowler, wickets, deliveries) in newList:
    print(batsman + ',' + bowler + ',' + str(wickets) + ',' + str(deliveries))

'''
for (batsman, bowler, wickets, deliveries) in newList:
    f.write(str(batsman + ',' + bowler + ',' + str(wickets) + ',' + str(deliveries))+'\n')
'''
f.close()
