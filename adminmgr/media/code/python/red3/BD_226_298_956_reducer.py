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
    line_val = line.split("@")
    key, val = line_val[0], line_val[1]
    try:
        l = val.split(";")
        runs = int(l[0])
        balls = int(l[1])
    except ValueError:
        continue
    if current_key == key:
        current_count1 += runs
        current_count2 += balls
    else:
        if (current_key != ""):
            if(current_key.split("|")[0] not in temp):
                if(current_count2>=10):
                    temp[current_key.split("|")[0]] = [(current_key.split("|")[1],round((current_count1 / current_count2) * 100,2), current_count1)]
            else:
                if(current_count2>=10):
                    temp[current_key.split("|")[0]].append((current_key.split("|")[1],round((current_count1 / current_count2) * 100,2), current_count1))
        current_count1 = runs
        current_count2 = balls
        current_key = key
        

if (current_key == key):
    if(current_key.split("|")[0] not in temp):
        if(current_count2>=10):
            temp[current_key.split("|")[0]] = [(current_key.split("|")[1],round((current_count1 / current_count2) * 100,2), current_count1)]
    else:
        if(current_count2>10):
            temp[current_key.split("|")[0]].append((current_key.split("|")[1],round((current_count1 / current_count2) * 100,2), current_count1))



for i in temp:
    list1 = tuple()
    x = max(temp[i],key=lambda item:item[1])
    for j in range(len(temp[i])):
        if(temp[i][j][1]==x[1]):
            if(len(list1)):
                if(list1[2]<temp[i][j][2]):
                    list1 = temp[i][j]
            else:
                list1 = temp[i][j]
    print("%s,%s"%(i,list1[0]))
    


