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
		runs = my_list[7]
		extra_runs = my_list[8]
	elif(isBall == 'info' and my_list[1] == 'venue'):
		if(my_list[2][0]=="\""):
			venue = my_list[2] + ',' + my_list[3]
		else:
			venue = my_list[2]

	if(isBall == 'ball'):
		if(extra_runs == '0'):
			print('%s;%s\t%s' % (venue,batsman,runs))
