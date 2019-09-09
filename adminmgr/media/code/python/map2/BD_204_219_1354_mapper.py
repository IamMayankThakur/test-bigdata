#!/usr/bin/python3
import sys
import csv
infile=sys.stdin

for line in infile:
	line=line.strip()
	mylist=line.split(',')
	if(mylist[0]=="ball"):
		runs=int(mylist[7])+int(mylist[8])
		k=mylist[6]+","+mylist[4]+","+str(runs)
		print('%s\t%s'%(k,'1'))   	
    	
    	
 
