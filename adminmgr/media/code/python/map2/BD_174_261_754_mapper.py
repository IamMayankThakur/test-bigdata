#!/usr/bin/python3
import sys

for line in sys.stdin:
    line = line.strip()
    data = line.split(",")
    list1 = []
    if(len(data) > 8):
        list1.append([data[4], data[6], data[7], data[8]])
    for i in list1:
        print(i[0], i[1], int(i[2]), int(i[3]), sep=",")
