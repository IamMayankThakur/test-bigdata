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
    sr=float(line[2])
    venue=line[0]
    if venue not in d:
        d[venue]=None
        maxm=0
    elif(sr>maxm):
            batsman=line[1]
            d[venue]=[batsman]
            maxm=sr
    elif(sr==maxm): 
            batsman=line[1]
            d[venue].append(batsman)
fields=sorted(list(d.keys()))
for i in fields:
	print("%s,%s" % (i,d[i][0]))    
