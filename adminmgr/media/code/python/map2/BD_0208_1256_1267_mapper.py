#!/usr/bin/python3
import sys

deliveries = 5;
dictionary = {}
lines=[]

for line in sys.stdin:
    lines.append(line)

for line in lines: 
	mapf = map(str,line.split(","))
	row=list(mapf)
	if((row[0]!="ball")==False):
		if (row[4],row[6]) not in dictionary:
			dictionary[(row[4],row[6])]=1
		elif(dictionary[(row[4],row[6])]<=deliveries):
			dictionary[(row[4],row[6])] = dictionary[(row[4],row[6])] + 1

for line in lines:
	mapf = map(str,line.split(","))
	row=list(mapf)
	if((row[0]!="ball")==False):
		if(dictionary[(row[4],row[6])]>deliveries):
			print(row[6],row[4],int(row[7]),int(row[8]),sep=",")