#!/usr/bin/python3
import csv
from operator import itemgetter
from collections import defaultdict
from functools import reduce
import sys

dic = defaultdict(lambda : [0,0])
for row in sys.stdin:
    row = row.strip()
    row = row.split("\t")
    #print(row)
    key = (row[0],row[1])
    value = (row[2],row[3])
    try:
        balls = int(row[3])
    except ValueError:
        continue
    
    try:
        runs = int(row[2])
    except ValueError:
        continue

    dic[key][0] += runs
    dic[key][1] += balls

    #print(dic)

#print(dic)
dic = filter(lambda x: x[1][1] >= 10, sorted(dic.items(), key = lambda x: ( x[1][0]/x[1][1], x[1][0] ), reverse=True))

newmap = defaultdict(lambda : (0, 0 , ''))
for kv in dic:
    temp = (kv[1][0]/kv[1][1], kv[1][0], kv[0][1])
    if temp > newmap[kv[0][0]]:
        # newmap[kv[0][0]] = (kv[1][0]/kv[1][1], kv[1][0], kv[0][1])
        newmap[kv[0][0]] = temp

#  print(dic)
#newmap1 = sorted(newmap.items()
result = []
for kv in newmap:
    #print(kv, ',', newmap[kv][2], sep='')
    result.append((kv,newmap[kv][2]))

result = sorted(result,key = lambda x:x[0])
for res in result:
  print(res[0],',',res[1],sep='')
