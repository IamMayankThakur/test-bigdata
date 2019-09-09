#!/usr/bin/python3
import sys

dictionary={}

for line in sys.stdin:

    line = line.strip()
    row = list(map(str,line.split(",")))
    extra=int(row[3])

    if (row[0],row[1]) in dictionary:
        dictionary[(row[0],row[1])][0] = dictionary[(row[0],row[1])][0] + int(row[2])+ int(extra)
        dictionary[(row[0],row[1])][1] = dictionary[(row[0],row[1])][1] + 1

    else:
        dictionary[(row[0],row[1])]=[int(row[2]),1]
        dictionary[(row[0],row[1])][0] = dictionary[(row[0],row[1])][0] + extra


sortedlist = sorted(dictionary,key=lambda k:(-dictionary[k][0],dictionary[k][1],k[0]))


for i in sortedlist:
    print(i[0],i[1],dictionary[i][0],dictionary[i][1],sep=",")