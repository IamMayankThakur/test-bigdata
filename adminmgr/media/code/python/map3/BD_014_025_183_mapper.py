#!/usr/bin/python3
import sys
import csv

fxMap = []
venue = ""

with sys.stdin as f:
        data = csv.reader(f)
        for line in data:
            runs = 0
            batsman = ""
            if(line[0] == "info" and line[1] == "venue"):
                venue = line[2]
            if(line[0] == "ball" and int(line[8]) == 0):
                batsman = line[4]
                runs = line[7]
                fxMap.append(list([venue, batsman, runs]))

for i in range(len(fxMap)):
    print(fxMap[i][0] ,";", fxMap[i][1], ";", fxMap[i][2], sep="")
