#!/usr/bin/python3
import sys
import csv
infile =sys.stdin
d={}
batsman=None
for line in infile:
    line=line.strip()
    line=line.split("\t") 
    #print(line)
    k=float(line[2])
    venue=line[0]
    if venue not in d:
        d[venue]=None
        maxma=0
    elif(k>maxma):
            batsman=line[1]
            d[venue]=[batsman]
            maxma=k
    elif(k==maxma): 
            batsman=line[1]
            d[venue].append(batsman)
field=sorted(list(d.keys()))
for i in field:
	print("%s,%s" % (i,d[i][0]))    
