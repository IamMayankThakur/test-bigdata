import fileinput
from operator import itemgetter

myDictionaryXY = {}
myDictionaryYX = {}
myList=[]


for line in fileinput.input():
    line = line.strip()
    line = line.split("\t")
    batsman, bowler, runs = line[0], line[1], int(line[2])

    if bowler not in myDictionaryXY:
        myDictionaryXY[bowler] = {}
        myDictionaryXY[bowler][batsman] = [0,0]    # [Runs, Number of Balls Faced]

    elif bowler in myDictionaryXY:
        if batsman not in myDictionaryXY[bowler]:
            myDictionaryXY[bowler][batsman] = [0,0]

    myDictionaryXY[bowler][batsman][0] += runs
    myDictionaryXY[bowler][batsman][1] += 1


    if batsman not in myDictionaryYX:
        myDictionaryYX[batsman] = {}
        myDictionaryYX[batsman][bowler] = [0,0]    # [Runs, Number of Balls Faced]

    elif batsman in myDictionaryYX:
        if bowler not in myDictionaryYX[batsman]:
            myDictionaryYX[batsman][bowler] = [0,0]

    myDictionaryYX[batsman][bowler][0] += runs
    myDictionaryYX[batsman][bowler][1] += 1




for bowler in list(myDictionaryXY):                # Remove batsman-bowler pair who has less than 5 deliveries in between.
    for batsman in list(myDictionaryXY[bowler]):
        if myDictionaryXY[bowler][batsman][1] < 5:
            del myDictionaryXY[bowler][batsman]

for batsman in list(myDictionaryYX):                # Remove batsman-bowler pair who has less than 5 deliveries in between.
    for bowler in list(myDictionaryYX[batsman]):
        if myDictionaryYX[batsman][bowler][1] < 5:
            del myDictionaryYX[batsman][bowler]



for bowler in myDictionaryXY:
    tempBowler = bowler
    tempList = list(myDictionaryXY[bowler].keys())
    if len(tempList) > 0:
        tempBatsman = tempList[0]

        for batsman in myDictionaryXY[bowler]:
            if (myDictionaryXY[bowler][batsman][0] == myDictionaryXY[bowler][tempBatsman][0]):
                if (myDictionaryXY[bowler][batsman][1] == myDictionaryXY[bowler][tempBatsman][1]):
                    if tempBatsman < batsman:
                        tempBatsman = batsman

                elif (myDictionaryXY[bowler][batsman][1] < myDictionaryXY[bowler][tempBatsman][1]):
                    tempBatsman = batsman

            elif (myDictionaryXY[bowler][batsman][0] > myDictionaryXY[bowler][tempBatsman][0]):
                tempBatsman = batsman

        myList.append((tempBowler,tempBatsman, myDictionaryXY[bowler][tempBatsman][0], myDictionaryXY[bowler][tempBatsman][1]))


for batsman in myDictionaryYX:
    tempBatsman = batsman
    tempList = list(myDictionaryYX[batsman].keys())
    if len(tempList) > 0:
        tempBowler = tempList[0]

        for bowler in myDictionaryYX[batsman]:
            if (myDictionaryYX[batsman][bowler][0] == myDictionaryYX[batsman][tempBowler][0]):
                if (myDictionaryYX[batsman][bowler][1] == myDictionaryYX[batsman][tempBowler][1]):
                    if tempBowler < bowler:
                        tempBowler = bowler

                elif (myDictionaryYX[batsman][bowler][1] > myDictionaryYX[batsman][tempBowler][1]):
                    tempBowler = bowler

            elif (myDictionaryYX[batsman][bowler][0] > myDictionaryYX[batsman][tempBowler][0]):
                tempBowler = bowler

        myList.append((tempBowler,tempBatsman, myDictionaryYX[batsman][tempBowler][0], myDictionaryYX[batsman][tempBowler][1]))



newList = list(set(myList))
newList2 = sorted(newList, key=itemgetter(2), reverse=True)

for (bowler, batsman, wickets, deliveries) in newList2:
    print(bowler + ',' + batsman + ',' + str(wickets) + ',' + str(deliveries))
