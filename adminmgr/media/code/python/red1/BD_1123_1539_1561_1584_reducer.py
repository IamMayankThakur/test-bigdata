#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
infile = sys.stdin
from signal import signal, SIGPIPE, SIG_DFL
d={}
signal(SIGPIPE,SIG_DFL) 
for line in infile:
    line = line.strip()
    l = line.split('  ')
    pair=l[0].split(',')
	
    batbowl=(pair[0],pair[1])
	
    if batbowl not in d:
        d[batbowl]=[0,1]
        if pair[2]=="1":
            d[batbowl][0]+=1
    else:
        d[batbowl][1]+=1
        if pair[2]=="1":
            d[batbowl][0]+=1

new_d = {i:j for i,j in d.items() if j[1]>5 }

names = sorted(new_d.items())
deliveries=sorted(names,key= lambda kv : kv[1][1])
wickets = sorted(deliveries,key=lambda kv : kv[1][0],reverse=True)

for i in wickets:
    s = i[0][0]+','+i[0][1]+','+str(i[1][0])+','+str(i[1][1])
    print(s)








