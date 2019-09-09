#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
mydict={}
#for line in maplist:
for line in sys.stdin:
	line = line.strip()
	line_split = line.split(",")
	key=line_split[0]+","+line_split[1]
	val=int(line_split[2])
	
	
	if key not in mydict:
		mydict[key]=[0,0]
	mydict[key][0]+=1
	mydict[key][1]+=val
	
	
'''
	
for key in mydict:
		print(key,mydict[key][1],mydict[key][0],sep=",")
'''		

#print(mydict)
sortedlist=sorted(mydict,key=lambda k:(-mydict[k][1],mydict[k][0],k))

for k in sortedlist:

	if(mydict[k][0]>5):
		print(k,mydict[k][1],mydict[k][0],sep=",")
#print("aa")
