#!/usr/bin/python
import csv
from operator import itemgetter
import sys

dict1 = {}
for line in sys.stdin:
    line = line.strip()
    line_val = line.split("\t")
    bb = line_val[0].split(",")
    key = (bb[0],bb[1])
    out = int(bb[2])
    val = line_val[1]
    if key in dict1:
    	dict1[key][0] += int(val)
        if out == 1:
            dict1[key][1] += 1
    else:
    	dict1[key] = [1,0]
        if out == 1:
            dict1[key][1] += 1            
for key in list(dict1):
    if dict1[key][0] <= 5:
        del dict1[key]
l = sorted(dict1.iteritems())
print (l).sort(key=)