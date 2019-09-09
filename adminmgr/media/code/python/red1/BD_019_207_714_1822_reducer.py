#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
import itertools
current_count = 0
current_key = ""
current_count = 0
current_key = ""
lana = dict()
i = 0
for line in sys.stdin:
    line = line.strip()
    line_val = line.split(",")
    key1,key2,key3,key4 = line_val[0], line_val[1],int(line_val[2]),int(line_val[3])
    if((key1,key2) in lana.keys()):
        d = lana[key1,key2]
        lana[key1,key2] = (key3 + d[0] , key4+d[1]) 
    else:
        lana[key1,key2] = (key3,key4)
for k,v in list(lana.items()):
    v1,v2 = v[0],v[1]
    if(v[1] < 6):
        del lana[k]
old = []
l=sorted(lana.items(),key = lambda x:(-x[1][0] , x[1][1]))
print(l)