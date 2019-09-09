#!/usr/bin/python3
import sys
import csv
infile=sys.stdin

for line in infile:
	line=line.strip()
	mylist=line.split(',')
	if(mylist[0]=="ball"):
		k=mylist[4]+","+mylist[6]+","
		if(mylist[9]=='""' or mylist[9] == "run out" or mylist[9] =="retired hurt"):
			k=mylist[4]+","+mylist[6]+","+"0"
		else:
			k=mylist[4]+","+mylist[6]+","+"1"
		print('%s\t%s'%(k,'1'))
