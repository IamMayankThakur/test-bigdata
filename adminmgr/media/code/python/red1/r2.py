##!/usr/bin/python3
import sys

p=dict()
for rec in sys.stdin:
    rec=eval(rec)
    if((rec[0],rec[1]) in p):
        p[(rec[0],rec[1])][0]+=rec[2]
        p[(rec[0],rec[1])][1]+=rec[3]
    else:
        p[(rec[0],rec[1])]=[rec[2],rec[3]]

list_p=[]
for pair in p:
    if(p[pair][1]>5):
        list_p.append(pair+tuple(p[pair]))

list_p.sort(key= lambda r: (-r[2],r[3],r[0]))

for i in list_p:
    #print(i)
    print(i[0],i[1],i[2],i[3],sep=",")
