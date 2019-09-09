#!/usr/bin/python3
import sys
#import csv
#infile = sys.stdin
#next(infile)
venue=''
leng=[]
#fuel column index 8
for line in sys.stdin:
    line = line.strip()
    line = line.split(',')
    if(line[1]=='venue'):
      if(len(line)==3):
        venue=line[2]
      else:
        venue=line[2]+','+line[3]
    if(line[0]=='ball'):
        batsman=line[4]
        run=line[7]
        if(line[8]=='0'):
           print(venue,',',line[4],',',int(run),sep='')
