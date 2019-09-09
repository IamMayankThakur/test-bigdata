#!/usr/bin/python
import sys
import csv
read_line = sys.stdin

for line in read_line:
	line = line.strip();
	my_list = line.split(',')
	if(my_list[0]=='ball'):
		batsman = my_list[4]
		bowler = my_list[6]
		runs = int(my_list[7]) + int(my_list[8])
		#record = (batsman, bowler, runs, 1)
		record = bowler+","+batsman+","+ str(runs)
		print('%s\t%s' % (record, 1))
		
		
		
