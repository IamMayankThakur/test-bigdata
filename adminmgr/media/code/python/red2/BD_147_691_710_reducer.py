#!/usr/bin/python3
import csv
from operatore import * 
import sys
import itertools
lst=dict()
i=0
for line in sys.stdin:
    line =line.strip()
    line_val = line.split(",")
    key1,key2,key3,key4 = line_val[0],line_val[1],int(linearray[3]),int(linearray[4])

    if((key1,key2) in lst.keys()):
        d=lst[key1,key2]
        lst[key1,key2] (key3,key4)
for k,v in list(lst.items()):
    v1,v2 = v[0],v[1]
    if(v[1]<6):
        del lst[k]
old=[]
l=sorted(lst.items(),key=lambda x:(-x[1][0],x[1][1]))
for k in range(len(l)):
    print(l[k][0][0]+ "," + l[k][0][1]+str(l[k][1][0])+","+k[1][1]