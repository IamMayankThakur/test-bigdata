import fileinput

myDictionary = {}
myList=[]


for line in fileinput.input():
    line = line.strip()
    line = line.split("\t")
    venue = line[0]
    values = line[1].split(",")
    batsman = values[0]
    runs = int(values[1])

    if venue not in myDictionary:
        myDictionary[venue] = {}
        myDictionary[venue][batsman] = [0,0]    # [Runs, Number of Balls Faced]
        myDictionary[venue][batsman][1] += 1
        myDictionary[venue][batsman][0] += runs

    elif venue in myDictionary:
        if batsman in myDictionary[venue]:
            myDictionary[venue][batsman][1] += 1
            myDictionary[venue][batsman][0] += runs

        elif batsman not in myDictionary[venue]:
            myDictionary[venue][batsman]=[0,0]
            myDictionary[venue][batsman][1] += 1
            myDictionary[venue][batsman][0] += runs


for venue in list(myDictionary):                # Remove player names who has faced less than 10 deliveries
    for batsman in list(myDictionary[venue]):
        if myDictionary[venue][batsman][1] < 10:
            del myDictionary[venue][batsman]


for venue in myDictionary:                      # Calculating the Strike Rate
    for batsman in myDictionary[venue]:
        strikeRate = (myDictionary[venue][batsman][0]*100) / myDictionary[venue][batsman][1]
        myDictionary[venue][batsman].append(strikeRate)


for venue in myDictionary:
    tempVenue = venue
    tempList = list(myDictionary[venue].keys())
    tempBatsman = tempList[0]

    for batsman in myDictionary[venue]:
        if(myDictionary[venue][batsman][2] == myDictionary[venue][tempBatsman][2]):
            if(myDictionary[venue][batsman][0] > myDictionary[venue][tempBatsman][0]):
                tempBatsman = batsman

        elif(myDictionary[venue][batsman][2] > myDictionary[venue][tempBatsman][2]):
            tempBatsman = batsman

    myList.append((tempVenue,tempBatsman))


myList.sort()

for (venue, batsman) in myList:
    print(venue + ',' + batsman)
