#!/usr/bin/python3
import sys
venue = ""

for line in sys.stdin:
    line = line.strip()
    data = line.split(',')
    arr = []
    if(data[1] == 'venue'):
        if(data[2][0] == "\"" and data[3] != ""):
            venue = data[2]+","+data[3]
        else:
            venue = data[2]

    elif(data[0] == 'ball'):
        arr.append([venue, data[4], data[7], data[8]])

    for i in arr:
        print("%s\t%s\t%s\t%s" % (i[0], i[1], i[2], i[3]))
