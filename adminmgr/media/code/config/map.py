#!/usr/bin/python
import sys
import csv
read_line = sys.stdin

for line in read_line:
	line = line.strip();
	my_list = line.split(',')
	if(my_list[0]=='ball'):
		if(my_list[9] != 'run out'):
			out = my_list[9]
		else:
			out = ' '
		batsman = my_list[4]
		bowler = my_list[6]
		runs = int(my_list[7]) + int(my_list[8])
		record = (batsman, bowler, 1, runs, out)
		print('%s,%s,%s,%s,%s' % (batsman, bowler, 1, runs, out))
		
		
		
