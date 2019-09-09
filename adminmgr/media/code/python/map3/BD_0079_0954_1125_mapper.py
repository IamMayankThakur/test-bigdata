#!/usr/bin/python3
import sys
import csv
infile =sys.stdin
d={}
venue=None
for line in infile:
    line=line.strip()
    line=line.split(",")
    #print(line)
    if(line[1]=="venue"):
        if(len(line)==4):
         venue=line[2]+","+line[3]
        else:
         venue=line[2]
        if(venue not in d):
            d[venue]={}
    elif(line[0]=="ball"):
        batsman=line[4]
        if batsman not in d[venue]:
            d[venue][batsman]=[0,0]
        
        runs=int(line[7])
        extras=int(line[8])
        d[venue][batsman][0]+=runs
        if(extras==0 or (extras>=1 and runs>0)):
            d[venue][batsman][1]+=1
#print(d)
for field in d:
    for batsman in d[field]:
     if(d[field][batsman][1]>=10):
      sr=d[field][batsman][0]/d[field][batsman][1]
      print('{}\t{}\t{}'.format(field,batsman,sr))
	    
