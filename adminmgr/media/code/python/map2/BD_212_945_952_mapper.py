#!/usr/bin/python3


import sys
import csv

inputfile = sys.stdin

for line in inputfile:
	line=line.strip()
	list_col = line.split(',')
	col0 = list_col[0]
	if(col0=='ball'):
		batsman=list_col[4]
		bowler=list_col[6]
		runs=int(list_col[7])+int(list_col[8])
		tup=(bowler,batsman,1,runs)

		print(tup)
