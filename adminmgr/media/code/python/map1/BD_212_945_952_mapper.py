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
		out=list_col[9]
		if(out=='caught' or out=='lbw' or out=='bowled' or out=='caught and bowled' or out=='hit wicket' or out=='stumped' or out=='obstructing the field'):
			tup=(batsman,bowler,1,1)
		else:
			tup=(batsman,bowler,1,0)

		print(tup)
	
	


	
