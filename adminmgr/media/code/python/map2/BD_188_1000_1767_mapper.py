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
		runs = int(my_list[7]) + int(my_list[8])
		print('%s,%s\t%s' % (bowler,batsman,runs))
