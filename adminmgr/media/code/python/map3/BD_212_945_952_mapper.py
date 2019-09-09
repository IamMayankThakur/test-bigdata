#!/usr/bin/python3


import sys
import csv

inputfile = sys.stdin

ven=''

for line in inputfile:
	line=line.strip()
	list_col = line.split(',')
	col0 = list_col[0]
	if(col0=='info' and list_col[1]=='venue'):
		if(len(list_col)>3):
			ven=list_col[2]+','+list_col[3]
		else:
			ven=list_col[2]
	elif(col0=='ball'):
		batsman=list_col[4]
		runs=int(list_col[7])
		if(int(list_col[8])==0):
			tup=(ven,batsman,1,runs)
			print(tup)
