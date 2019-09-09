#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)
val=1

#fuel column index 8
#print("heyyy")
for line in infile:
	line = line.strip()
	my_list = line.split(',')
	if(my_list[0]=="info" and my_list[1]=="venue"):
		if(len(my_list)>3):
			venue1=my_list[2]+','+my_list[3]
		else:
			venue1=my_list[2]
	else:
		if(my_list[0]=="ball" and int(my_list[8])==0):
			print('%s,%s,%s,%d'% (my_list[4],my_list[7],venue1,val))








