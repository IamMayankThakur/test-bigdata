#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

wickvery={}
final={}

for line in sys.stdin:
    line = line.strip()
    line_val = line.split("\t")
    key = line_val[0]
    val = line_val[1]
    keysplit=key.split(",")
    bbpair=keysplit[0]+","+keysplit[1]
    outstat=keysplit[2]
    
    if bbpair in wickvery.keys():
    	wickvery[bbpair][0]=1+wickvery[bbpair][0]
    else:
    	wickvery[bbpair]=[1,0]
    if(outstat=='1'):
    	wickvery[bbpair][1]=wickvery[bbpair][1]+1

for x, y in wickvery.items():
	if y[0]>5:
		final[x]=y
		
listkeys = sorted(final.items())
listdel = sorted(listkeys, key = lambda x: x[1][0])
listwick = sorted(listdel, key = lambda x: x[1][1], reverse = True)

for i in listwick:
	print (i[0]+","+str(i[1][1])+","+str(i[1][0]))    

