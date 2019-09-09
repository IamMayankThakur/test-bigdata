#!/usr/bin/python3
import sys
import csv
infile=sys.stdin
key=""
dict={}
venue=""
batsman=""
runs=0
for line in infile:
	line = line.strip()
	line = line.split(",")
	if (line[0]=="info" and line[1]=="venue"):
		if(line[2][0]=='"'):
			key=line[2]+","+line[3]
		else:
			key=line[2]
	if (line[0]=="ball" and line[8]=='0'):
		batsman = line[4]
		runs=line[7]
		print(key,batsman,runs,sep="\t")  	
