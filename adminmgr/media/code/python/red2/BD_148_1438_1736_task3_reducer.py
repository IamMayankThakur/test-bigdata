#!/usr/bin/python3

import csv
import sys
import itertools

lana = dict()

for line in sys.stdin:
    line_val = line.split(",")
    key1,key2,key3,key4,key5 = line_val[0], line_val[1],int(line_val[2]),int(line_val[3]),int(line_val[4])

    if((key1,key2) in lana.keys()):
        d = lana[(key1,key2)]
        lana[(key1,key2)] = (key3 + key4 + d[0], key5 + d[1])
    else:
        lana[(key1,key2)] = (key3 + key4, key5)


for k,v in list(lana.items()):
    v1,v2 = v[0],v[1]
    if(v[1] < 6):
        del lana[k]


old = []
for i in sorted(lana.items(),key = lambda x:(-x[1][0],x[1][1],x[0][0])) :
    old.append(i)

for i in range(len(old)):
    print(old[i][0][0],old[i][0][1],old[i][1][0],old[i][1][1],sep=",")
