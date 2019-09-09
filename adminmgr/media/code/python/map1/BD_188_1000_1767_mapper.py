#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
#next(infile)

#fuel column index 8
for line in infile:
	line = line.strip()
	my_list = line.split(',')
	isBall = my_list[0]
	if(isBall == 'ball'):
		batsman = my_list[4]
		bowler = my_list[6]
		runs = my_list[7]
		try:
			nod = my_list[9]
			bd = my_list[10]
		except ValueError:
			continue
		if( len(nod)==2 or nod == "run out" or nod == "retired hurt"):
			print('%s,%s\t%s' % (batsman,bowler,'0'))     
		else:
			print('%s,%s\t%s' % (batsman,bowler,'1'))     
