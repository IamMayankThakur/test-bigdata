#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

runvery={}
final={}

for line in sys.stdin:
    line = line.strip()
    line_val = line.split("\t")
    key, val = line_val[0], line_val[1]
    keysplit=key.split(",")
    bbpair=keysplit[0]+","+keysplit[1]
    runs=int(keysplit[2])
    if bbpair in runvery.keys():
    	runvery[bbpair][0]=1+runvery[bbpair][0]
    	runvery[bbpair][1]=runs+runvery[bbpair][1]
    else:
    	runvery[bbpair]=[1,0]
    	runvery[bbpair][1]=runs+runvery[bbpair][1]
	

for x, y in runvery.items():
	if y[0]>5:
		final[x]=y
		
	
listkeys = sorted(final.items())
listdel = sorted(listkeys, key = lambda x: x[1][0])
listrun = sorted(listdel, key = lambda x: x[1][1], reverse = True)


for i in listrun:
	print (i[0]+","+str(i[1][1])+","+str(i[1][0]))
    

