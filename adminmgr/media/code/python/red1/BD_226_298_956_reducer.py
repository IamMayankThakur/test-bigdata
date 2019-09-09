#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
import operator
from six import iteritems

temp = dict()
current_count1 = 0
current_count2 = 0
current_key = ""
for line in sys.stdin:
    line = line.strip()
    line_val = line.split(",")
    key, val = line_val[0], line_val[1]
    try:
        l = val.split(";")
        balls = int(l[1])
        wickets = int(l[0])
    except ValueError:
        continue
    if current_key == key:
        current_count1 += balls
        current_count2 += wickets
    else:
        if (current_key != "" and current_count1>5):
            temp[current_key] = [-1*current_count2,current_count1]
            
        current_count1 = balls
        current_count2 = wickets
        current_key = key
        

if (current_key == key and current_count1>5):
    temp[current_key] = [-1*current_count2,current_count1]

temp2 = sorted(sorted(iteritems(temp)), key=operator.itemgetter(1))
for i in temp2:
    print("%s,%s,%s,%s" % (i[0].split("|")[0],i[0].split("|")[1],-1*i[1][0],i[1][1]))
