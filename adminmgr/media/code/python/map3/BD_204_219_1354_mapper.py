#!/usr/bin/python3
import sys
import csv
infile=sys.stdin
venue = 0
for line in infile:
	line=line.strip()
	mylist=line.split(',')
	
	if mylist[1] == "venue":
		if(mylist[2][0] == "\""):
			venue = mylist[2] + "," + mylist[3]
		else:
			venue = mylist[2]

		
		
		
			
		#print(venue)
	if mylist[0] == "ball" and mylist[8] == "0" :
		k = venue + ";" + mylist[4] + ";" + mylist[7]
		print('%s\t%s'%(k,'1'))
